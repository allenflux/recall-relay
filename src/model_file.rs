//! Ollama 和 llama.cpp 对同一份 EmbeddingGemma 权重使用了不同的 GGUF 字段名。
//! 只改临时副本的文件头。张量的顺序、量化格式和字节均保留，原模型卷可以只读挂载。

use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};
use std::{
    env,
    fs::File,
    io::{self, Cursor, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};
use tempfile::NamedTempFile;

const TAG: &str = "embeddinggemma:300m-qat-q8_0";
const SHA256: &str = "ed705a9a986a4c6c26bb9cdd92e232ec1ca1525ae952e41c8600793d6e28ebb3";
const HEADER_SHA256: &str = "36645126bd3388921fba778c4dca2a3979aa7f43c0e15607256afd41f67a4492";
// 此版本文件中第一个张量的数据起点，已经按 GGUF 要求对齐到了 32 字节。
const DATA_START: usize = 6_530_176;
const FILE_SIZE: u64 = 338_014_336;

pub(crate) fn embedding_path(model: &str) -> Result<NamedTempFile> {
    ensure!(model == TAG, "内置推理支持 {TAG}，当前配置为 {model}");
    let path = if let Some(path) = env_path("HISTORY_EMBEDDING_MODEL_PATH") {
        path
    } else {
        env_path("HISTORY_EMBEDDING_MODEL_DIR")
            .or_else(|| env_path("OLLAMA_MODELS"))
            .or_else(|| env_path("HOME").map(|home| home.join(".ollama/models")))
            .context("请设置 HISTORY_EMBEDDING_MODEL_DIR 或 HISTORY_EMBEDDING_MODEL_PATH")?
            .join("blobs")
            .join(format!("sha256-{SHA256}"))
    };
    let mut source = File::open(&path)
        .with_context(|| format!("无法打开 {TAG} 权重 {}，请核对模型卷", path.display()))?;
    ensure!(
        source.metadata()?.len() == FILE_SIZE,
        "{TAG} 权重大小应为 {FILE_SIZE} 字节"
    );
    let mut header = vec![0; DATA_START];
    source
        .read_exact(&mut header)
        .context("GGUF 文件头不完整")?;

    // 先核对头部，才能使用这份固定模型的布局解析元数据。复制张量时再验证整份文件。
    let mut hash = Sha256::new();
    hash.update(&header);
    ensure!(
        hex::encode(hash.clone().finalize()) == HEADER_SHA256,
        "GGUF 文件头与 {TAG} 不符"
    );
    let mut output = NamedTempFile::new().context("无法在临时目录中创建向量模型副本")?;
    output.write_all(&adapt_header(&header)?)?;
    let mut buffer = vec![0; 256 * 1024];
    loop {
        let n = source.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        output
            .write_all(&buffer[..n])
            .context("临时目录空间不足或模型副本写入失败")?;
    }
    ensure!(
        hex::encode(hash.finalize()) == SHA256,
        "GGUF 权重校验失败，期望 {TAG} 的 SHA-256 为 {SHA256}"
    );
    output.flush()?;
    Ok(output)
}

fn env_path(key: &str) -> Option<PathBuf> {
    env::var_os(key)
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
}

fn u32_le(input: &mut Cursor<&[u8]>) -> io::Result<u32> {
    let mut b = [0; 4];
    input.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

fn u64_le(input: &mut Cursor<&[u8]>) -> io::Result<u64> {
    let mut b = [0; 8];
    input.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn string(input: &mut Cursor<&[u8]>) -> Result<String> {
    let len = u64_le(input)? as usize;
    let start = input.position() as usize;
    let bytes = input
        .get_ref()
        .get(start..start + len)
        .context("GGUF 字段长度越界")?;
    input.set_position((start + len) as u64);
    Ok(std::str::from_utf8(bytes)?.to_owned())
}

fn put_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u64).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

// GGUF 的数值类型宽度固定；字符串单独携带长度，数组先写元素类型与个数。
fn skip_values(input: &mut Cursor<&[u8]>, kind: u32, count: u64) -> Result<()> {
    let width = match kind {
        0 | 1 | 7 => 1,
        2 | 3 => 2,
        4..=6 => 4,
        10..=12 => 8,
        8 => {
            for _ in 0..count {
                let len = u64_le(input)?;
                input.seek(SeekFrom::Current(len as i64))?;
            }
            return Ok(());
        }
        9 if count == 1 => {
            let element = u32_le(input)?;
            let len = u64_le(input)?;
            return skip_values(input, element, len);
        }
        _ => bail!("未知的 GGUF 值类型 {kind}"),
    };
    input.seek(SeekFrom::Current((width * count) as i64))?;
    Ok(())
}

fn adapt_header(header: &[u8]) -> Result<Vec<u8>> {
    let mut input = Cursor::new(header);
    input.set_position(8); // 跳过 GGUF 标志和版本号。
    let tensors = u64_le(&mut input)?;
    let fields = u64_le(&mut input)?;
    let mut out = header[..16].to_vec();
    out.extend_from_slice(&(fields + 4).to_le_bytes());
    for _ in 0..fields {
        let key = string(&mut input)?;
        put_string(&mut out, &key.replacen("gemma3.", "gemma-embedding.", 1));
        let kind = u32_le(&mut input)?;
        out.extend_from_slice(&kind.to_le_bytes());
        let start = input.position() as usize;
        skip_values(&mut input, kind, 1)?;
        if key == "general.architecture" {
            put_string(&mut out, "gemma-embedding");
        } else {
            out.extend_from_slice(&header[start..input.position() as usize]);
        }
    }
    // 平均池化后原模型还有两层投影。新加载器用这四个维度来构建它们。
    for (field, value) in [
        ("dense_2_feat_in", 768u32),
        ("dense_2_feat_out", 3072),
        ("dense_3_feat_in", 3072),
        ("dense_3_feat_out", 768),
    ] {
        put_string(&mut out, &format!("gemma-embedding.{field}"));
        out.extend_from_slice(&4u32.to_le_bytes()); // GGUF_TYPE_UINT32
        out.extend_from_slice(&value.to_le_bytes());
    }
    for _ in 0..tensors {
        let name = string(&mut input)?;
        put_string(
            &mut out,
            match name.as_str() {
                "dense.0.weight" => "dense_2.weight",
                "dense.1.weight" => "dense_3.weight",
                other => other,
            },
        );
        let start = input.position() as usize;
        let dims = u32_le(&mut input)?;
        input.seek(SeekFrom::Current(i64::from(dims) * 8 + 12))?;
        out.extend_from_slice(&header[start..input.position() as usize]);
    }
    out.resize(out.len().next_multiple_of(32), 0);
    Ok(out)
}
