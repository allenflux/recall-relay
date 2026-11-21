//! 封装 llama.cpp 的同步调用。工作线程在这里加载 GGUF 并计算整句向量。

use super::embedding::{DIMENSIONS, normalize};
use anyhow::{Context, Result, bail, ensure};
use llama_cpp_sys_2 as llama;
use std::{
    ffi::c_void,
    ptr::NonNull,
    sync::{
        Once,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Instant,
};
const MODEL_WIDTH: usize = 768;
const CONTEXT: usize = 2048;

// 两个句柄分别负责一份 C 资源。Engine 的字段按声明顺序析构，上下文先于模型释放。
// 它们只活在向量工作线程，无需对裸指针标记 Send 或 Sync。
struct Model(NonNull<llama::llama_model>);
impl Drop for Model {
    fn drop(&mut self) {
        unsafe { llama::llama_model_free(self.0.as_ptr()) };
    }
}
struct ContextHandle(NonNull<llama::llama_context>);
impl Drop for ContextHandle {
    fn drop(&mut self) {
        unsafe { llama::llama_free(self.0.as_ptr()) };
    }
}

pub(super) struct Engine {
    context: ContextHandle,
    model: Model,
}

struct Interrupt<'a> {
    stop: &'a AtomicBool,
    cancelled: &'a AtomicBool,
    until: Option<Instant>,
}

impl Interrupt<'_> {
    fn expired(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
            || self.cancelled.load(Ordering::Relaxed)
            || self.until.is_some_and(|end| Instant::now() >= end)
    }
}

unsafe extern "C" fn abort(data: *mut c_void) -> bool {
    // infer 在同步和移除回调之后才离开，指针指向的 Interrupt 始终有效。
    unsafe { (&*data.cast::<Interrupt<'_>>()).expired() }
}

unsafe extern "C" fn continue_loading(_: f32, stop: *mut c_void) -> bool {
    // 加载回调的语义相反：true 表示继续。stop 来自工作线程始终持有的 Arc。
    unsafe { !(&*stop.cast::<AtomicBool>()).load(Ordering::Relaxed) }
}

/// llama.cpp 的默认回调会在每次提问时写多条调试日志。统一交给服务日志过滤，
/// 保留模型加载和计算失败的诊断；普通请求不把提示词或向量写入日志。
unsafe extern "C" fn log_backend(
    level: llama::ggml_log_level,
    text: *const std::ffi::c_char,
    _: *mut c_void,
) {
    if text.is_null()
        || !matches!(
            level,
            llama::GGML_LOG_LEVEL_ERROR | llama::GGML_LOG_LEVEL_WARN
        )
    {
        return;
    }
    // C 库保证消息以 NUL 结尾，且仅在这次回调期间有效。
    let message = unsafe { std::ffi::CStr::from_ptr(text) }.to_string_lossy();
    if level == llama::GGML_LOG_LEVEL_ERROR {
        tracing::error!(target:"recall_relay::embedding", "{}", message.trim_end());
    } else {
        tracing::warn!(target:"recall_relay::embedding", "{}", message.trim_end());
    }
}

impl Engine {
    pub(super) fn load(path: &std::path::Path, stop: &AtomicBool) -> Result<Self> {
        static INIT: Once = Once::new();
        INIT.call_once(|| unsafe {
            llama::llama_log_set(Some(log_backend), std::ptr::null_mut());
            llama::llama_backend_init();
        });
        let filename = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())?;
        let mut settings = unsafe { llama::llama_model_default_params() };
        let mut no_gpus = [std::ptr::null_mut()];
        settings.devices = no_gpus.as_mut_ptr();
        settings.n_gpu_layers = 0;
        settings.progress_callback = Some(continue_loading);
        settings.progress_callback_user_data = std::ptr::from_ref(stop).cast_mut().cast();
        let raw = unsafe { llama::llama_model_load_from_file(filename.as_ptr(), settings) };
        let model = Model(NonNull::new(raw).context("llama.cpp 无法加载 EmbeddingGemma GGUF")?);
        ensure!(
            unsafe { llama::llama_model_n_embd_out(raw) } == MODEL_WIDTH as i32,
            "模型输出应为 768 维"
        );
        let mut params = unsafe { llama::llama_context_default_params() };
        params.embeddings = true;
        params.pooling_type = llama::LLAMA_POOLING_TYPE_MEAN;
        params.attention_type = llama::LLAMA_ATTENTION_TYPE_NON_CAUSAL;
        params.offload_kqv = false;
        params.op_offload = false;
        params.n_ctx = CONTEXT as u32;
        // 双向注意力必须一次看到整句；大于该上限的输入直接拒绝，不裁剪提问。
        params.n_batch = CONTEXT as u32;
        params.n_ubatch = CONTEXT as u32;
        let threads = thread::available_parallelism().map_or(1, |n| n.get().min(4)) as i32;
        params.n_threads = threads;
        params.n_threads_batch = threads;
        let ctx = unsafe { llama::llama_init_from_model(raw, params) };
        let context = ContextHandle(NonNull::new(ctx).context("无法创建 EmbeddingGemma 上下文")?);
        if stop.load(Ordering::Relaxed) {
            bail!("向量模型启动已取消");
        }
        Ok(Self { context, model })
    }

    pub(super) fn infer(
        &mut self,
        input: &str,
        stop: &AtomicBool,
        cancelled: &AtomicBool,
        until: Option<Instant>,
    ) -> Option<Vec<f64>> {
        let mut interrupt = Interrupt {
            stop,
            cancelled,
            until,
        };
        if interrupt.expired() {
            return None;
        }
        let ctx = self.context.0.as_ptr();
        let mut tokens = vec![0; CONTEXT];
        let vocab = unsafe { llama::llama_model_get_vocab(self.model.0.as_ptr()) };
        // true,true 读取 GGUF 的 BOS/EOS 配置，也按模型词表识别特殊 token。
        // 传长度而非 C 字符串，可正确处理提问中的 U+0000。
        let count = unsafe {
            llama::llama_tokenize(
                vocab,
                input.as_ptr().cast(),
                input.len().try_into().ok()?,
                tokens.as_mut_ptr(),
                CONTEXT as i32,
                true,
                true,
            )
        };
        if count <= 0 || count as usize > CONTEXT {
            return None;
        }
        let mut batch = unsafe { llama::llama_batch_get_one(tokens.as_mut_ptr(), count) };
        // 均值池化需要整句每个 token 的隐状态，包括首尾特殊 token。
        let mut all_outputs = vec![1_i8; count as usize];
        batch.logits = all_outputs.as_mut_ptr();
        // 本次 FFI 调用期间 token 数组、回调状态和模型都存活。
        let status = unsafe {
            llama::llama_set_abort_callback(
                ctx,
                Some(abort),
                std::ptr::from_mut(&mut interrupt).cast(),
            );
            let code = llama::llama_encode(ctx, batch);
            llama::llama_synchronize(ctx);
            llama::llama_set_abort_callback(ctx, None, std::ptr::null_mut());
            code
        };
        if status != 0 || interrupt.expired() {
            return None;
        }
        let output = NonNull::new(unsafe { llama::llama_get_embeddings_seq(ctx, 0) })?;
        // 该指针归上下文所有。下一次推理可能覆盖它，所以先复制成 Rust 自己的 Vec。
        let mut values =
            unsafe { std::slice::from_raw_parts(output.as_ptr(), MODEL_WIDTH) }.to_vec();
        unit_length(&mut values)?;
        values.truncate(DIMENSIONS);
        unit_length(&mut values)?;
        normalize(values.into_iter().map(f64::from).collect())
    }
}

/// 沿用历史向量的计算精度：用 float32 累加平方、float64 开方，再将系数转回 float32。
fn unit_length(values: &mut [f32]) -> Option<()> {
    let sum: f32 = values.iter().map(|x| x * x).sum();
    if !sum.is_finite() || sum <= 0.0 {
        return None;
    }
    let factor = (1.0 / f64::from(sum).sqrt().max(1e-12)) as f32;
    for x in values {
        *x *= factor;
    }
    Some(())
}
