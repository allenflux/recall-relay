# 单机 Docker Compose 部署

这套配置启动 Relay、Redis 和本机 CPU Ollama。Relay 在宿主机 `127.0.0.1:8000` 接收请求。Ollama 与 Relay 共用网络命名空间，通过容器内 `127.0.0.1:11434` 接收降级请求。Redis 通过 Compose 的内部网络提供历史查询，Redis 和 Ollama 均未发布宿主机端口。 [Docker Compose 网络与启动顺序](https://docs.docker.com/compose/how-tos/startup-order/)

## 启动

在项目根目录准备配置文件。

```sh
cp .env.compose.example .env.compose
chmod 600 .env.compose
```

用 `openssl rand -hex 32` 生成 `REDIS_PASSWORD`，十六进制密码可直接组成 Redis URL。`UPSTREAM_URL` 默认使用 Python 版的网关地址，`FALLBACK_MODEL` 默认是 `gemma4-31b-heretic`，两者都可以在 `.env.compose` 中覆盖。显式设置 `FALLBACK_MODEL=` 可关闭网关备用模型。`RELAY_PORT` 是本机发布端口。

```sh
docker compose --env-file .env.compose config --quiet
docker compose --env-file .env.compose up -d --build
docker compose --env-file .env.compose ps
curl -fsS http://127.0.0.1:8000/healthz
```

在 VPS 上使用本机反向代理时，Relay 保持监听 `127.0.0.1`，由反向代理提供公网 HTTPS 入口。生产客户端通过 HTTPS 发送令牌和请求内容。

第一次启动时，Ollama 会下载 SmolVLM 权重并创建 CPU 模型 `recall-relay-cpu:vision`。Relay 可先处理主模型和历史请求，Ollama 就绪后才承接 CPU 降级。Redis AOF 和 Ollama 权重保存在各自的 Docker 命名卷中。

```sh
docker compose --env-file .env.compose logs --tail=100 ollama
docker compose --env-file .env.compose exec ollama ollama show recall-relay-cpu:vision
```

### 透传客户端的上游 key

在 backend 将 `CUSTOM_LLM_BASE_URL` 改为 Relay 的 HTTPS 入口并保留 `/v1`。Relay 把客户端提供的 `Authorization` 原样传给 `UPSTREAM_URL` 对应的网关。网关负责首发模型和网关备用模型的鉴权，返回 `401` 或 `403` 时 Relay 直接结束当前请求。

只有包含单个非空 `Authorization` 的请求可读写历史。Redis 按该请求头的 SHA-256 指纹隔离历史，本地 CPU 仅接收协议所需的请求头。

## 测试请求

将 `RELAY_CLIENT_TOKEN` 设为有效的网关 key。首发模型由请求体的 `model` 选择。下面的 your-model 需要替换为你的上游模型 ID。

```sh
curl -fsSN http://127.0.0.1:8000/v1/chat/completions \
  -H "Authorization: Bearer ${RELAY_CLIENT_TOKEN:?set RELAY_CLIENT_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data-binary '{"model":"your-model","stream":true,"max_tokens":64,"messages":[{"role":"user","content":"请用一句话回复：单机部署已连通。"}]}'
```

本部署为统一入口配置了 Redis。聊天请求按精确历史、近似历史、已配置的网关备用模型、CPU 的顺序处理符合条件的首发故障。近似仅适用于一张内联图。多图精确历史未命中时，原图和顺序会交给后续可用的视觉模型。单机默认最多 4 个活跃 Relay 请求，其中 CPU 降级最多 1 个。

### 本地 CPU 向量匹配

在 `.env.compose` 中设置 `HISTORY_EMBEDDING_MODE=shadow` 可先观察向量命中，设为 `replay` 可用于故障回放。向量模式开启后，近似匹配使用向量。默认 `off` 时继续使用现有规则。重建 Relay 和 Ollama 后配置生效。Ollama 从本地 `/api/embed` 提供 `embeddinggemma:300m-qat-q8_0`，首次启用会下载约 338 MB 权重，模型保存在现有命名卷。 [Ollama 模型标签](https://ollama.com/library/embeddinggemma/tags)

向量计算只接收符合近似条件的单图提问文本。回放仍按原图内容和请求范围筛选，向量索引随 Redis 对应热记录淘汰。启用后新完成的合格请求才会写入向量。只想运行 CPU 向量模型时，可设 `CPU_FALLBACK_ENABLED=false`，Ollama 就会跳过 SmolVLM 下载和创建。两个模型都启用时，可同时加载。

### MySQL 长期历史

使用已有 MySQL 时，设置 `MYSQL_HOST`、`MYSQL_PASSWORD`，以及所需的 `MYSQL_PORT`、`MYSQL_DATABASE` 和 `MYSQL_USER`。默认端口为 3306，默认库名和应用用户均为 `recall_relay`。Relay 应用用户需要在指定数据库里创建、查询和写入 `recall_relay_history` 的权限。

使用 Compose 内置 MySQL 时，在受保护的 `.env.compose` 中设置独立的 `MYSQL_PASSWORD` 和 `MYSQL_ROOT_PASSWORD`。下面启用 Redis 和 MySQL 向量回放，并关闭两级备用生成模型。数据库只监听 Compose 网络，数据写入 `mysql-data` 命名卷。MySQL 命中后按需回填 Redis 热缓存。已有数据库需要 `ALTER` 和 `INDEX` 权限来自动补齐向量列和联合索引。

```sh
export HISTORY_EMBEDDING_MODE=replay CPU_FALLBACK_ENABLED=false FALLBACK_MODEL=
docker compose --env-file .env.compose -f compose.yaml -f deploy/compose/mysql.yaml config --quiet
docker compose --env-file .env.compose -f compose.yaml -f deploy/compose/mysql.yaml up -d --build
```

使用 MySQL overlay 后，后续 Compose 更新和查看命令继续带上两个 `-f` 参数。MySQL 初始化时使用这些密码创建账号。已有数据卷上的密码变更需要先在数据库里执行，再更新 Compose 配置。

### 使用模拟上游验收

Rust 模拟上游位于 examples/mock_upstream.rs。准备独立的环境文件，在其中设置 `MOCK_UPSTREAM_API_KEY`、`REDIS_PASSWORD` 和不同于其他部署的 `RELAY_PORT`。`MOCK_UPSTREAM_API_KEY` 只传给 Compose 中的模拟上游服务。若仅验证历史与网关备用模型，可在该环境文件中设置 CPU_FALLBACK_ENABLED=false 跳过权重下载。测试请求携带该 key 的 Bearer `Authorization`。

```sh
docker compose --env-file .env.compose.mock -p recall-relay-mock \
  -f compose.yaml -f deploy/compose/mock.yaml up -d --build
```

请求的 `model` 可填 `mock-primary`。先发送一次正常请求给历史预热，再用相同请求体加 `X-Request-ID: mock-outage-primary` 验证精确回放。另一条新请求使用同一标记时会走模拟备用模型。`mock-outage-all` 让模拟上游的两个模型都返回 503，可继续验证本机 CPU。`model: mock-model-not-found` 验证终止降级的错误路径。

## 更新和停止

重建 Relay 时一起重建 Ollama，使其加入新 Relay 容器的网络命名空间。

```sh
docker compose --env-file .env.compose up -d --build --force-recreate relay ollama
```

停止时使用 `down`，Redis 数据和 Ollama 权重仍保存在卷里。 [Docker Compose 停止选项](https://docs.docker.com/reference/cli/docker/compose/down/)

```sh
docker compose --env-file .env.compose down
```
