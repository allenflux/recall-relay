#!/bin/sh
# 一次性准备命名卷里的模型。只有这个进程需要向卷里写文件。
set -eu
umask 022

case "${HISTORY_NEAR_MODE:-replay}/${HISTORY_EMBEDDING_MODE:-off}" in
    shadow/shadow|shadow/replay|replay/shadow|replay/replay) ;;
    *) echo "跳过模型准备，由 Relay 按配置启动"; exit 0 ;;
esac

if [ "${HISTORY_EMBEDDING_MODEL:-embeddinggemma:300m-qat-q8_0}" != "embeddinggemma:300m-qat-q8_0" ]; then
    echo "内置向量推理仅支持 embeddinggemma:300m-qat-q8_0" >&2
    exit 1
fi
digest=$(cat /usr/local/share/recall-relay/embeddinggemma.sha256)
store=/var/lib/recall-model-store
model_dir=${HISTORY_EMBEDDING_MODEL_DIR:-$store/models}
target=$(realpath -m -- "${HISTORY_EMBEDDING_MODEL_PATH:-$model_dir/blobs/sha256-$digest}")
case "$target" in
    "$store"/*) ;;
    *) echo "卷外自备模型由 Relay 启动时校验：$target"; exit 0 ;;
esac

valid_model() {
    [ -f "$1" ] && printf '%s  %s\n' "$digest" "$1" | sha256sum --check --status
}
if valid_model "$target"; then
    echo "已校验并复用模型：$target"
    exit 0
fi

dir=$(dirname -- "$target")
mkdir -p -- "$dir"
# 旧项目和新项目可能共用这个卷。锁住目录后再检查一次，避免同时下载。
exec 9<"$dir"
flock -x 9
if valid_model "$target"; then
    echo "已复用其他实例准备好的模型：$target"
    exit 0
fi

# 下载到同一目录的临时文件，断网或校验失败时不会把残缺内容交给 Relay。
temp=$(mktemp -- "$dir/.embeddinggemma.XXXXXX")
trap 'rm -f -- "$temp"' EXIT
trap 'exit 1' HUP INT TERM
echo "正在从 Ollama 官方仓库下载 EmbeddingGemma（约 338 MB）"
curl --fail --location --silent --show-error --proto '=https' --proto-redir '=https' \
    --connect-timeout 15 --max-time 900 --retry 3 \
    --output "$temp" "https://registry.ollama.ai/v2/library/embeddinggemma/blobs/sha256:$digest"
if ! valid_model "$temp"; then
    echo "下载的模型 SHA-256 校验失败，请重新启动模型准备服务" >&2
    exit 1
fi
chmod 0644 "$temp"
mv -T -- "$temp" "$target"
echo "已校验模型并写入：$target"
