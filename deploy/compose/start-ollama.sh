#!/bin/sh
set -eu

base_model=hf.co/ggml-org/SmolVLM-256M-Instruct-GGUF:Q8_0
cpu_model=recall-relay-cpu:vision
cpu_enabled=${CPU_FALLBACK_ENABLED:-true}
embedding_model=${HISTORY_EMBEDDING_MODEL:-embeddinggemma:300m-qat-q8_0}
serve_address=${OLLAMA_HOST:-127.0.0.1:11434}
embedding_enabled=false
if [ "${HISTORY_NEAR_MODE:-replay}" != off ] && [ "${HISTORY_EMBEDDING_MODE:-off}" != off ]; then
    embedding_enabled=true
fi
if [ "$cpu_enabled" = false ] && [ "$embedding_enabled" = false ]; then
    export OLLAMA_HOST="$serve_address"
    exec ollama serve
fi

# 下载权重时，Relay 已经可以处理主模型请求。先用临时端口准备权重，
# 等模型就绪后再开放正式端口，防止 CPU 兜底误用尚未准备好的模型。
OLLAMA_HOST=127.0.0.1:11435
export OLLAMA_HOST
ollama serve &
preparation_pid=$!
modelfile=''

cleanup() {
    kill "$preparation_pid" 2>/dev/null || true
    wait "$preparation_pid" 2>/dev/null || true
    if [ -n "$modelfile" ]; then
        rm -f "$modelfile"
    fi
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

attempts=0
until ollama list >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if ! kill -0 "$preparation_pid" 2>/dev/null || [ "$attempts" -ge 30 ]; then
        echo 'Ollama did not start for model preparation' >&2
        exit 1
    fi
    sleep 1
done

if [ "$cpu_enabled" = true ]; then
    if ! ollama show "$cpu_model" >/dev/null 2>&1; then
        if ! ollama show "$base_model" >/dev/null 2>&1; then
            ollama pull "$base_model"
        fi
        modelfile=$(mktemp)
        printf 'FROM %s\nPARAMETER num_gpu 0\nPARAMETER num_ctx 2048\n' "$base_model" > "$modelfile"
        ollama create "$cpu_model" -f "$modelfile"
    fi
    ollama show "$cpu_model" >/dev/null
fi

if [ "$embedding_enabled" = true ]; then
    if ! ollama show "$embedding_model" >/dev/null 2>&1 && ! timeout 120 ollama pull "$embedding_model"; then
        echo 'Embedding model preparation failed. Continuing without embeddings.' >&2
    elif [ "$cpu_enabled" = true ]; then
        OLLAMA_MAX_LOADED_MODELS=2
        export OLLAMA_MAX_LOADED_MODELS
    fi
fi

cleanup
trap - EXIT TERM INT
OLLAMA_HOST=$serve_address
export OLLAMA_HOST
exec ollama serve
