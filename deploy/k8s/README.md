# RecallRelay Kubernetes 部署

基础部署在 Relay Pod 内运行一个 Rust 容器。Redis 和 MySQL 各有独立的 StatefulSet，分别使用 1 GiB 和 10 GiB 的 PVC。Rust Relay 在集群内的 8000 端口提供统一入口。所有 Relay 副本应使用同一套网关、策略版本和历史命名空间。

## 构建和配置

在项目根目录构建镜像。把示例仓库替换成集群可拉取的镜像仓库，并把 [kustomization.yaml](kustomization.yaml) 中 `images` 的 `newName`、`newTag` 设成同一地址和标签。混合架构集群按目标节点构建对应平台镜像。

```sh
docker build -t registry.example.com/team/recall-relay:0.1.0 .
docker push registry.example.com/team/recall-relay:0.1.0
```

ConfigMap 的 `UPSTREAM_URL` 配置唯一上游。Relay 转发客户端原请求，符合条件的成功回答会异步写入历史。主请求遇到可回放故障时，先查精确缓存，再按配置查同范围的近似缓存。普通聊天请求命中后返回历史回答，缓存中没有可用回答时返回错误。[ConfigMap 配置](config.yaml)

Redis 热缓存默认保留 24 小时、每个前缀最多 1024 条。Relay 在 MySQL 中自动创建 `recall_relay_history`，用命名空间哈希和请求哈希组成主键，长期保存符合入库条件的成功结果。MySQL 历史没有 TTL 或按条数淘汰，部署时需根据写入量规划 PVC 容量和备份。使用已有 MySQL 时，部署清单中可移除 `mysql.yaml`，在 ConfigMap 中设置该实例的地址、端口、库名和用户。先创建数据库并授予 Relay 用户建表及读写权限。仅使用 Redis 时，同样可移除 `mysql.yaml`，并将 `MYSQL_HOST` 设置为空字符串。

### 原生 CPU 向量

基础清单默认使用 `HISTORY_EMBEDDING_MODE=off`。向量模式使用 Relay 内部的 llama.cpp 加载 GGUF。测试或调试时可将 `RELAY_LOG_CONTENT` 设为 `false` 关闭请求和响应正文日志，各方向默认预览最多 2048 个原始字节。

[原生向量 overlay](../k8s-overlays/native-vectors/kustomization.yaml) 为 Relay 只读挂载已有 PVC `recall-relay-embedding-models`，开启 `replay`，并为模型和请求预留更多内存。先在相同命名空间准备好权重卷。Rust 在卷的 `blobs/` 下按 `embeddinggemma:300m-qat-q8_0` 对应的固定 SHA-256 读取 GGUF，并校验整份文件。如果卷内放的是单个 GGUF，在 overlay 的 ConfigMap 中配置 `HISTORY_EMBEDDING_MODEL_PATH: /var/lib/recall-models/文件名.gguf`。运行用户 10001 需要具备读取权限。基础清单为 Relay 的 /tmp 挂载 768 MiB 的磁盘 emptyDir，供模型加载时写入约 338 MB 的临时兼容副本，原始 GGUF 保存在只读 PVC 中。多节点多副本部署时，选择可供对应节点共同挂载的存储方案。原 Python 清单若用的是 Pod 的 `emptyDir`，务必在替换 Pod 前将需要的权重转存到 PVC，或预先准备相同版本的权重。

## 创建 Secret 和应用清单

以下命令示例部署基础清单。使用原生向量时，把 `kubectl apply -k deploy/k8s` 改为 `kubectl apply -k deploy/k8s-overlays/native-vectors`。

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

业务后端在集群内将 `CUSTOM_LLM_BASE_URL` 设为 `http://recall-relay.recall-relay.svc:8000/v1`，使用自己的网关凭据。Relay 将 `Authorization` 原样传给固定网关，由网关鉴权。

```sh
kubectl -n recall-relay get pods,svc,pvc
kubectl -n recall-relay exec statefulset/recall-relay-mysql -- sh -ec '
  MYSQL_PWD="$MYSQL_PASSWORD" mysql --protocol=TCP --host=127.0.0.1 \
    --user="$MYSQL_USER" --database="$MYSQL_DATABASE" --execute="SHOW INDEX FROM recall_relay_history"'
kubectl -n recall-relay port-forward svc/recall-relay 8000:8000
```

在另一个终端访问 `http://127.0.0.1:8000/healthz` 检查 Relay。用同一个转发端口发起实际 Chat Completions 请求可以检查网关和鉴权。`/healthz` 检查 Relay 进程状态，Redis、MySQL 和模型链路由各自的探针及请求验证。

普通 ConfigMap 或 Secret 更新会在容器重建后生效。调整配置后重新 rollout 对应工作负载。MySQL 镜像在空数据卷上初始化数据库和账号，已有数据库需要在 MySQL 内修改密码后再更新 Secret。Redis 和 MySQL 这两个单实例规格适用于联调和小流量试运行，生产高可用需要单独规划。
