# RecallRelay · Rust 版

客户端始终访问一个地址，默认是 `http://127.0.0.1:8000`。服务把请求发给你配置的上游。设置 `REDIS_URL` 后，同一个端口上的聊天请求自动获得故障回放能力。上游可使用 HTTP 或 HTTPS。需要向外提供 HTTPS 时，可由反向代理终止 TLS，再转给这个端口。

## 先跑通一个请求

在本项目目录打开终端。第一次编译会下载 Cargo 依赖。下面用项目附带的模拟模型，示例令牌 `demo` 只用于这次本地练习。

**终端一** 启动模拟上游。它监听 8091，扮演部署时的模型服务。

```sh
MOCK_UPSTREAM_API_KEY=demo cargo run --locked --example mock_upstream
```

**终端二** 启动 RecallRelay。这是客户端连接的服务，监听 8000。

```sh
UPSTREAM_URL=http://127.0.0.1:8091/v1/chat/completions cargo run --locked
```

**终端三** 发一次请求。响应的 `choices[0].message.content` 应为 `Mock response from demo-model.`。

```sh
curl -sS http://127.0.0.1:8000/v1/chat/completions \
  -H 'Authorization: Bearer demo' \
  -H 'Content-Type: application/json' \
  -d '{"model":"demo-model","messages":[{"role":"user","content":"你好"}]}'
```

用 RustRover 打开本项目时，可以直接在它的终端里运行以上命令。调试时在 `dispatch` 打断点，再发送一次请求，就能看到请求路径和请求头。

## 同一个 POST 怎样返回 JSON 或 SSE

聊天客户端发送的都是 `POST /v1/chat/completions`。上面的请求省略了 `stream`，模拟服务一次返回完整 JSON。想看 SSE，在同一条命令的请求体中加上 `"stream":true`，将 curl 的 `-sS` 改为 `-NsS`，终端就会按收到的顺序打印 `data:` 事件。

启用历史后的聊天处理器会检查 `User-Agent`。以 `ComfyUI-Sulphur-Prompt-Enhancer/` 开头的标识可识别 Sulphur 插件。提示词回显还要求是非流式、单回答的文本优化请求，并且前面的降级阶段均未得到结果。插件内部的语言和风格探针按其自己的规则处理。

## 在相同地址验证故障回放

准备一个可访问的 Redis。假设它监听本机 6379，在终端二按 Ctrl+C 停止 Relay，然后用以下配置重新启动。客户端 URL 和请求体保持一致。

```sh
UPSTREAM_URL=http://127.0.0.1:8091/v1/chat/completions \
REDIS_URL=redis://127.0.0.1:6379/0 \
POLICY_EPOCH=demo-v1 \
cargo run --locked
```

先在终端三重新发送上面的正常请求，等回答收完。Relay 会在后台保存通过校验的回答。然后添加模拟服务识别的故障标记，再发同一个请求。

```sh
curl -si http://127.0.0.1:8000/v1/chat/completions \
  -H 'Authorization: Bearer demo' \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: mock-outage-all' \
  -d '{"model":"demo-model","messages":[{"role":"user","content":"你好"}]}'
```

模拟上游这次返回 503。历史记录就绪后，客户端会看到 `200 OK` 和 `X-Failover-Source: history`。请求 ID 可以变化，授权信息、业务请求头、路径、查询参数和聊天参数都参与历史隔离。

## 沿着一次请求读代码

| 顺序 | 打开文件 | 读代码时可以观察什么 |
| --- | --- | --- |
| 1 | [src/main.rs](src/main.rs) | 找到唯一的 `TcpListener::bind`。看 `.await` 如何等待服务，以及 `?` 如何将启动错误交回调用者。 |
| 2 | [src/lib.rs](src/lib.rs) | 从 `dispatch` 读起。健康检查就地返回，启用历史的聊天请求交给聊天处理器，普通请求交给转发器。 |
| 3 | [src/passthrough.rs](src/passthrough.rs) | 在 `forward` 里跟着一条请求看 method、原始路径、查询串、请求头和字节如何发往固定主机。 |
| 4 | [src/relay/mod.rs](src/relay/mod.rs) | `handle` 负责排队和解析，`conversation` 决定先调用哪个模型、何时才能查历史。 |
| 5 | [src/history/identity.rs](src/history/identity.rs) 与 [src/history/mod.rs](src/history/mod.rs) | `canonicalize` 借用请求，生成用于查找记录的指纹。`enqueue` 把可保存的回答交给后台，`lookup` 负责故障时取回。 |
| 6 | [src/relay/sse.rs](src/relay/sse.rs) 与 [src/delivery.rs](src/delivery.rs) | 观察上游分块、SSE 事件和下游写入之间的区别。一个事件可能横跨多个网络分块。 |

第一次读可以只走前三步。遇到 `Arc::clone`，看它包住的是哪个共享对象，例如连接池或信号量。遇到并发许可，找它最后被移到了哪里。Relay 已经发出响应头的时候，SSE 可能还在输出，因此许可跟着发送任务走，到结束、超时或断开时自动归还。

## 聊天请求的处理顺序

每次先尝试客户端指定的主模型。单个非空 `Authorization`、可确定身份的请求及通过校验的完整主模型回答，才具备历史读写条件。主模型鉴权失败会直接返回。如果主模型超时、限流、服务不可用或返回符合降级条件的空回答，Relay 在提交下游响应前依次尝试以下来源。

| 来源 | 条件与行为 |
| --- | --- |
| 精确历史 | 先查 Redis，再查可选的 MySQL。回放前再次校验保存的 JSON 或 SSE。 |
| 近似历史 | 单张内联图片可按相同范围和已识别的意图匹配。启用 Ollama 嵌入后改用有阈值的向量匹配。 |
| 网关备用模型 | 配置 `FALLBACK_MODEL` 后，向同一网关换模型尝试一次。成功时响应头标记为 `model`。 |
| 本机 CPU 模型 | 同时配置 `CPU_FALLBACK_URL` 和 `CPU_FALLBACK_MODEL` 后尝试本机服务。仅转发所需协议头与请求 ID，网关凭据留在网关一侧。成功时标记为 `cpu`。 |
| Sulphur 提示词 | 对符合条件的 ComfyUI Sulphur 非流式优化请求，可返回本次请求中的用户提示词，标记为 `prompt`。其余请求返回相应的可重试错误。 |

流式响应已经开始传输后，上游中断会终止当前响应。主模型正常返回、管理员关闭某个降级阶段或某个候选不符合条件时，会跳过后续或对应阶段。

## 常用配置

| 变量 | 用途 |
| --- | --- |
| `UPSTREAM_URL` | 完整聊天端点。普通请求沿用这个主机。也兼容只提供 `UPSTREAM_ORIGIN` 的配置。 |
| `RELAY_BIND` | 唯一监听地址，默认 `127.0.0.1:8000`。反向代理后的外部入口可另用 `INGRESS_ORIGIN` 声明，以检查转发环路。 |
| `REDIS_URL`、`REDIS_PREFIX`、`POLICY_EPOCH` | Redis 地址、历史命名空间和策略版本。与 Python 版共享既有记录时，使用原部署的前缀和策略值。 |
| `REQUEST_PATH` | 对客户端提供的聊天路径，默认 `/v1/chat/completions`。健康检查为 `/healthz`。上游使用标准聊天路径后缀时，可通过 `GET /v1/models` 查询对应模型列表。 |
| `MAX_INFLIGHT`、`MAX_BODY_BYTES` | 全服务共用的活跃请求上限，默认 16。单次请求体上限默认 10 MiB。聊天还支持 `MAX_PENDING`、`QUEUE_TIMEOUT_SECONDS` 和 `MAX_BUFFERED_BODY_BYTES`。 |
| `TOTAL_TIMEOUT_SECONDS`、`STREAM_TIMEOUT_SECONDS` | 默认请求总预算 60 秒。启用历史后，已经提交的聊天流有独立预算，默认 600 秒。非流式、SSE 启动和 Sulphur 的细分预算见 [src/config.rs](src/config.rs)。 |
| `MYSQL_HOST`、`MYSQL_PORT`、`MYSQL_USER`、`MYSQL_PASSWORD`、`MYSQL_DATABASE` | 配置长期存储。Redis 热缓存默认保留 24 小时、最多 1024 条。MySQL 命中后会回填 Redis。 |
| `HISTORY_NEAR_MODE`、`HISTORY_EMBEDDING_MODE` | 可选 `off`、`shadow` 或 `replay`。`shadow` 只观察近似命中。默认启用规则匹配，关闭嵌入。 |
| `RUST_LOG` | 例如 `recall_relay=debug` 可以查看降级阶段及命中计数。 |

## 验证与部署

```sh
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --all -- --check
```

有 `redis-server` 时测试会启动临时实例。若程序不在 PATH 下，可设置 `TEST_REDIS_BIN`。MySQL 集成用例需要独立的 MySQL 8.4 测试库，参数见 [src/archive.rs](src/archive.rs)。HTTP 集成用例启动本机临时端口，不需要真实模型或生产令牌。

容器部署和本机 Ollama 的配置见 [Compose 说明](deploy/compose/README.md) 和 [Kubernetes 说明](deploy/k8s/README.md)。
