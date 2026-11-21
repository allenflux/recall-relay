# 单机 Docker Compose 部署

默认启动 Rust Relay、Redis 和 MySQL。Relay 在宿主机 `127.0.0.1:8000` 提供统一入口。向量模式启用后，进程内的 llama.cpp 在 CPU 上读取 EmbeddingGemma 的 GGUF 权重，生成 384 维文本向量。向量计算只用于筛选历史回答。

## 启动

在项目根目录运行以下命令。初始化脚本以 [.env.compose.example](../../.env.compose.example) 为模板，生成只允许当前用户读写的 `.env`，内含随机的 Redis 和 MySQL 应用密码。文件已存在时脚本会保留已有配置。Docker Compose 默认读取项目目录下的 `.env`。

```sh
sh deploy/compose/init-env.sh
docker compose config --quiet
docker compose up -d --build --wait relay
docker compose ps
curl -fsS http://127.0.0.1:8000/healthz
```

`UPSTREAM_URL` 指定唯一上游。并发到达的有效请求直接转给上游，缓存只保存通过校验的完整回答。请求和响应正文默认各预览最多 2048 个原始字节，可将 `RELAY_LOG_CONTENT=false` 写入环境文件关闭正文日志。

`RELAY_PORT` 控制宿主机发布端口。客户端将原来的网关基础地址改为 Relay 入口，保留自己的凭据和请求体中的模型 ID。Relay 把客户端的 Authorization 转给固定网关，并按凭据隔离历史记录。

聊天上游返回 429、5xx 或原因未知的 HTTP 400 时，Relay 先查缓存。命中返回历史回答，普通聊天未命中则返回错误。明确的鉴权、权限、内容政策或模型不存在错误直接返回。SSE 已开始传输后遇到故障，当前流会结束。

```sh
curl -fsSN http://127.0.0.1:8000/v1/chat/completions \
  -H "Authorization: Bearer ${RELAY_CLIENT_TOKEN:?set RELAY_CLIENT_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data-binary '{"model":"your-model","stream":true,"messages":[{"role":"user","content":"请用一句话问好。"}]}'
```

### 读懂一条请求的日志

运行 `docker compose logs --tail 100 -f relay`，按同一个 `request_id` 跟踪一次调用。下表中的字段来自同一条请求的不同处理阶段。

| 日志名 | 怎样阅读 |
| --- | --- |
| `relay_stage` | `stage` 标识聊天、模型列表或通用 HTTP 转发。`event=attempt` 表示准备转发，`response` 表示收到响应，`failed` 或 `stream_failed` 配合 `reason` 定位失败原因。有上游响应时可查看 `upstream_status`。 |
| `relay_history` | `outcome=hit` 表示找到可回放的回答，`miss` 表示这次未返回历史。`reason` 可以区分精确命中、向量命中、缓存不可用等情况。 |

## 启用向量回放

在项目的 `.env` 中设置以下两项。Compose 会把同一套配置传给模型初始化服务和 Relay。默认 `HISTORY_EMBEDDING_MODE=off`，普通转发和精确回放都可以直接使用。

```dotenv
HISTORY_NEAR_MODE=replay
HISTORY_EMBEDDING_MODE=replay
```

两项均为 `shadow` 或 `replay` 时，一次性服务 `model-init` 会检查模型卷。默认权重是约 338 MB 的 `embeddinggemma:300m-qat-q8_0`。旧卷中的文件通过 SHA-256 校验即可离线复用。文件缺少或校验失败时，`model-init` 从 Ollama 官方仓库下载到同卷临时文件，校验通过后再移到正式路径。任一模式为 `off` 时跳过模型检查和下载。[官方模型页](https://ollama.com/library/embeddinggemma:300m-qat-q8_0)

首次启动、拉取更新或改动模式后，都在项目目录用同一条命令启动 Relay。

```sh
docker compose up -d --build --wait relay
```

这条命令会等待 `model-init` 成功退出再启动 Relay。`--no-deps` 会跳过这一步。下载失败时可用 `docker compose logs model-init` 查看原因，网络恢复后重新执行启动命令。

Relay 将 `ollama-data` 卷只读挂载到 `/var/lib/recall-model-store`。默认文件路径为卷内的 `models/blobs/sha256-<摘要>`。从旧项目迁移时，在 `.env` 将 `MODEL_VOLUME_NAME` 设为已有 Docker 卷的实际名称，并设 `MODEL_VOLUME_EXTERNAL=true`。复用旧历史时也要沿用原先的模型标签、阈值、策略版本、Redis 前缀和 MySQL 配置。

自备卷外 GGUF 时，可将文件只读挂载为 `/weights/embeddinggemma.gguf`，再设置 `HISTORY_EMBEDDING_MODEL_PATH=/weights/embeddinggemma.gguf`。也可挂载自备的模型目录并指定 `HISTORY_EMBEDDING_MODEL_DIR`。卷外权重由部署者提供，Relay 以 UID 10001 读取并校验。文件须匹配该标签固定的 SHA-256。[权重校验实现](../../src/model_file.rs)

Relay 在进程内加载模型并预热。为兼容原有 GGUF，它在容器的 `/tmp` 中创建约 338 MB 的临时副本。Compose 为 `/tmp` 配置了 768 MiB 的 tmpfs，实际占用计入 Relay 的内存上限（`RELAY_MEMORY_LIMIT`，默认 2 GiB）。

### 切换到新部署

新部署只需项目根目录的 `compose.yaml`。站点环境文件若位于其他路径，在命令中显式传入 `--env-file 路径`。模型卷可按上文复用；是否沿用历史数据库取决于部署所用的 Redis、MySQL 和策略配置。在正在提供服务的主机并行试跑时，使用独立的项目名和空闲的本机端口。

切换流量前先验证聊天转发和 SSE，再用测试凭据对同一张内联图预热。在受控的模拟故障下，更换为同范围的相似提问，检查是否收到 `X-Failover-Source: history`。切换前保留原镜像与环境文件，以便回滚。

## 数据存储

主配置中的 Redis 使用 AOF 持久化，数据挂载于 `redis-data`；MySQL 8.4 使用 `mysql-data` 并提供长期历史。两者都在项目私有网络中运行。Relay 等待 Redis PING 和 MySQL 应用账号的 SELECT 1 成功后启动。MySQL 初次初始化较慢，可用 `docker compose ps` 查看就绪状态。

应用账号仅访问 `MYSQL_DATABASE` 指定的库。MySQL 官方镜像在空卷首次初始化时生成 root 密码并写入 MySQL 容器的启动日志，日志访问权限应和管理员凭据相同。已有 MySQL 数据卷沿用库内的账号密码。更换环境文件里的密码时，要与数据库内的账号变更保持一致。

## 用模拟网关验收

准备独立环境文件 `.env.compose.mock`，可以用 `sh deploy/compose/init-env.sh .env.compose.mock` 为这个项目生成单独的 Redis 和 MySQL 密码，然后设置 `MOCK_UPSTREAM_API_KEY` 和空闲的 `RELAY_PORT`。模拟上游的实现见 [mock_upstream.rs](../../examples/mock_upstream.rs)。

```sh
docker compose --env-file .env.compose.mock -p recall-relay-mock \
  -f compose.yaml -f deploy/compose/mock.yaml up -d --build
```

用同一组 Bearer 凭据按顺序检查三种结果：

1. 以 `mock-primary` 发送普通文本聊天请求，预期收到 200 和模拟网关回答。等待异步写入完成。
2. 保持同一请求体和凭据，加上 `X-Request-ID: mock-rate-limit` 模拟上游 429，`mock-unknown-400` 模拟未知 400，`mock-outage-all` 模拟 503。预期仍是 200，带 `X-Failover-Source: history`，正文与第一步一致。
3. 保持故障标记，将问题改为没有匹配历史的普通聊天请求。预期收到模拟上游对应的 400、429 或 503。429 响应会保留上游的 `Retry-After`。

可用 `docker compose --env-file .env.compose.mock -p recall-relay-mock -f compose.yaml -f deploy/compose/mock.yaml down` 停止测试项目并保留命名卷。
