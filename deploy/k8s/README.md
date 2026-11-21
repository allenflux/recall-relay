# RecallRelay Kubernetes 部署

每个 RecallRelay Pod 运行 Relay 和 Ollama 两个容器。Relay 在 `8000` 提供集群内 Service，Ollama 只监听 Pod 内的 `127.0.0.1:11434`。Pod 的 init container 将 SmolVLM 权重准备到共享 `emptyDir`，并创建 `num_gpu 0` 的 `recall-relay-cpu:vision` 模型。Redis 和 MySQL 各自运行在独立的 StatefulSet，所有 Relay 副本通过集群内 Service 共用这两个库。

当前清单默认部署一个 Relay、一个 Redis 和一个 MySQL 副本。Redis 使用 1 GiB PVC 和 AOF，MySQL 使用 10 GiB PVC，两者使用集群的默认 StorageClass。调整 [relay.yaml](relay.yaml) 的 `spec.replicas` 就能增加 Relay 副本。已有的 ClusterIP Service 会把新连接分发给就绪的 Pod。各 Pod 共用 `POLICY_EPOCH` 和 `REDIS_PREFIX`。每个 Relay Pod 各有一份 Ollama 权重和 CPU 配额，新 Pod 首次启动需要能访问 Hugging Face 下载权重。网关出口还需允许 `UPSTREAM_URL` 指定的 HTTPS 服务。

同一个入口也接收普通 HTTP 请求并转发至固定网关；配置 Redis 后，聊天路径会使用历史与降级功能。

## 构建和配置

在项目根目录构建镜像。把示例仓库替换成集群可拉取的镜像仓库，并把 [kustomization.yaml](kustomization.yaml) 中 `images` 的 `newName`、`newTag` 设成同一地址和标签。混合架构集群按目标节点构建对应平台镜像。

```sh
docker build -t registry.example.com/team/recall-relay:0.1.0 .
docker push registry.example.com/team/recall-relay:0.1.0
```

ConfigMap 默认使用 Python 版的网关地址和备用模型 `gemma4-31b-heretic`。客户端用请求体的 `model` 选择首发模型。向量模式下，符合条件的故障依次尝试 Redis 精确、MySQL 精确、Redis 近似和 MySQL 近似，再进入备用模型。`model_not_found` 原样返回。更换网关时可调整 `UPSTREAM_URL`、`FALLBACK_MODEL` 和 `FALLBACK_MODEL_MULTIMODAL`。[ConfigMap 配置](config.yaml)

Redis 热缓存默认保留 24 小时、每个前缀最多 1024 条。Relay 在 MySQL 中自动创建 `recall_relay_history`，用命名空间哈希和请求哈希组成主键，长期保存符合入库条件的成功结果。MySQL 历史没有 TTL 或按条数淘汰，部署时需根据写入量规划 PVC 容量和备份。使用已有 MySQL 时，部署清单中可移除 `mysql.yaml`，在 ConfigMap 中设置该实例的地址、端口、库名和用户。先创建数据库并授予 Relay 用户建表及读写权限。仅使用 Redis 时，同样可移除 `mysql.yaml`，并将 `MYSQL_HOST` 设置为空字符串。

### 本地 CPU 向量匹配

在 [config.yaml](config.yaml) 将 `HISTORY_EMBEDDING_MODE` 从默认 `off` 调为 `shadow` 可以观察向量命中，调为 `replay` 可用于故障回放。向量模式开启后，近似匹配使用向量。默认 `off` 时继续使用现有规则。设为 `HISTORY_NEAR_MODE=off` 会关闭所有近似回放。更新配置后重建 Relay Pod。默认模型 `embeddinggemma:300m-qat-q8_0` 的权重约 338 MB。启用时 init container 通过 Ollama 模型仓库下载，每个 Pod 在同一 `emptyDir` 保存本地模型权重。 [Ollama 模型标签](https://ollama.com/library/embeddinggemma/tags)

Relay 从 Pod 内的 `127.0.0.1:11434/api/embed` 计算文本向量。它只在凭据、模型、参数和内联原图相同的范围匹配。启用后新完成的合格请求才会写入 Redis 和 MySQL 向量，同一 Redis 前缀下的各副本应统一模型和设置。MySQL 命中的记录会按需回填 Redis。使用已有 MySQL 时，给升级账号授予 `ALTER` 和 `INDEX` 以自动补齐向量字段。仅运行 CPU 向量模型时，设置 `CPU_FALLBACK_ENABLED: "false"`，init container 会跳过 SmolVLM 下载和创建。

## 创建 Secret 和应用清单

准备以下 Secret 源文件，值采用原始文本且末尾无换行。使用两个独立的 MySQL 密码，示例可分别用 `openssl rand -hex 32` 生成。Relay Pod 只读取 MySQL 应用密码。

| Secret | 键 | 内容 |
| --- | --- | --- |
| `recall-relay-secrets` | `REDIS_PASSWORD` | Redis 密码，建议用 `openssl rand -hex 32` 生成 |
| `recall-relay-secrets` | `REDIS_URL` | `redis://:<同一个 Redis 密码>@recall-relay-redis:6379/0`，密码如含保留字符需 URL 编码 |
| `recall-relay-mysql-secrets` | `MYSQL_PASSWORD` | MySQL 应用密码 |
| `recall-relay-mysql-secrets` | `MYSQL_ROOT_PASSWORD` | 内置 MySQL 的本地管理密码 |

```sh
kubectl apply -f deploy/k8s/namespace.yaml
kubectl -n recall-relay create secret generic recall-relay-secrets \
  --from-file=REDIS_PASSWORD=/secure/REDIS_PASSWORD \
  --from-file=REDIS_URL=/secure/REDIS_URL \
  --dry-run=client -o yaml | kubectl apply --server-side -f -
kubectl -n recall-relay create secret generic recall-relay-mysql-secrets \
  --from-file=MYSQL_PASSWORD=/secure/MYSQL_PASSWORD \
  --from-file=MYSQL_ROOT_PASSWORD=/secure/MYSQL_ROOT_PASSWORD \
  --dry-run=client -o yaml | kubectl apply --server-side -f -
kubectl apply -k deploy/k8s
kubectl -n recall-relay rollout status statefulset/recall-relay-redis
kubectl -n recall-relay rollout status statefulset/recall-relay-mysql --timeout=6m
kubectl -n recall-relay rollout status deployment/recall-relay --timeout=15m
```

## 验证

业务后端在集群内将 `CUSTOM_LLM_BASE_URL` 设为 `http://recall-relay.recall-relay.svc:8000/v1`，使用自己的网关凭据。Relay 将 `Authorization` 原样传给固定网关，由网关鉴权。首次模型下载进度可通过 `kubectl -n recall-relay logs deployment/recall-relay -c prepare-cpu-model` 查看。

```sh
kubectl -n recall-relay get pods,svc,pvc
kubectl -n recall-relay exec statefulset/recall-relay-mysql -- sh -ec '
  MYSQL_PWD="$MYSQL_PASSWORD" mysql --protocol=TCP --host=127.0.0.1 \
    --user="$MYSQL_USER" --database="$MYSQL_DATABASE" --execute="SHOW INDEX FROM recall_relay_history"'
kubectl -n recall-relay exec deployment/recall-relay -c ollama -- \
  ollama list
kubectl -n recall-relay port-forward svc/recall-relay 8000:8000
```

在另一个终端访问 `http://127.0.0.1:8000/healthz` 检查 Relay。用同一个转发端口发起实际 Chat Completions 请求可以检查网关和鉴权。`/healthz` 检查 Relay 进程状态，Redis、MySQL 和模型链路由各自的探针及请求验证。

普通 ConfigMap 或 Secret 更新会在容器重建后生效。调整配置后重新 rollout 对应工作负载。MySQL 镜像在空数据卷上初始化数据库和账号，已有数据库需要在 MySQL 内修改密码后再更新 Secret。Redis 和 MySQL 这两个单实例规格适用于联调和小流量试运行，生产高可用需要单独规划。
