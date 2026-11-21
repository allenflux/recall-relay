#!/bin/sh
# 为新 Compose 项目生成凭据，只有创建者能读取目标文件。
set -eu
umask 077

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
destination=${1:-"$root/.env"}
if [ -e "$destination" ]; then
    echo "保留已有配置 $destination"
    exit 0
fi
temp=$(mktemp "${destination}.XXXXXX")
trap 'rm -f "$temp"' EXIT
trap 'exit 1' HUP INT TERM

redis_password=$(openssl rand -hex 32)
mysql_password=$(openssl rand -hex 32)
while IFS= read -r line; do
    case $line in
        REDIS_PASSWORD=) line="REDIS_PASSWORD=$redis_password" ;;
        MYSQL_PASSWORD=) line="MYSQL_PASSWORD=$mysql_password" ;;
    esac
    printf '%s\n' "$line"
done < "$root/.env.compose.example" > "$temp"
# ln 在目标已存在时会失败，同时启动的两个初始化进程也不会覆盖已生成的密码。
ln "$temp" "$destination"
echo "已生成 $destination"
