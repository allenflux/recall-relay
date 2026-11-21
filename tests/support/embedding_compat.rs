//! 使用原 Python Relay 连接真实 Ollama CPU 生成的固定样本，检验换引擎后的行为。
//! 模型没有随测试发布，需在本机准备对应的 GGUF 后显式执行本用例。
use super::{DIMENSIONS, Embedder};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::Value;
use std::time::Duration;

const FIXTURE: &str = include_str!("../fixtures/embeddinggemma-ollama.json");
const BUDGET: Duration = Duration::from_secs(90);

fn legacy_vector(case: &Value) -> Result<Vec<f64>> {
    let bytes = STANDARD.decode(case["vector_f32_be"].as_str().context("缺少历史向量")?)?;
    ensure!(bytes.len() == DIMENSIONS * 4, "旧版向量应为 384 个大端 f32");
    Ok(bytes
        .chunks_exact(4)
        .map(|v| f32::from_be_bytes(v.try_into().unwrap()) as f64)
        .collect())
}

fn cosine(a: &[f64], b: &[f64]) -> Result<f64> {
    ensure!(
        a.len() == DIMENSIONS && b.len() == DIMENSIONS,
        "维度不是 384"
    );
    let norm = |v: &[f64]| v.iter().map(|x| x * x).sum::<f64>().sqrt();
    let (na, nb) = (norm(a), norm(b));
    ensure!(
        (na - 1.0).abs() < 1e-5 && (nb - 1.0).abs() < 1e-5,
        "向量范数异常 {na}/{nb}"
    );
    Ok(a.iter().zip(b).map(|(x, y)| x * y).sum::<f64>() / (na * nb))
}

#[tokio::test]
#[ignore = "需要本机官方 EmbeddingGemma GGUF"]
async fn native_matches_python_ollama_vectors() -> Result<()> {
    let fixture: Value = serde_json::from_str(FIXTURE)?;
    let cases = fixture["cases"].as_array().context("缺少兼容性样本")?;
    let model = fixture["model"].as_str().unwrap();
    let embedder = Embedder::new(model.to_owned())?;
    embedder.ready().await?;
    for case in cases {
        let text = case["prompt"].as_str().unwrap();
        let label = case["label"].as_str().unwrap();
        let reference = legacy_vector(case)?;
        let output = embedder
            .embed(text, BUDGET, true)
            .await
            .with_context(|| format!("原生引擎计算 {label} 失败"))?;
        let score = cosine(&reference, &output)?;
        ensure!(score >= 0.999, "{label} 与旧版基准余弦偏低 {score:.7}");
    }
    embedder.close().await;
    Ok(())
}
