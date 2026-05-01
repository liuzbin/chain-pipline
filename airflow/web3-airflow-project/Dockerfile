# 基于你现在正在使用的官方镜像
FROM apache/airflow:2.9.0

# 切换到 root 用户安装系统依赖（如果有的话）
USER root
RUN apt-get update && apt-get install -y git && apt-get clean

# 切换回 airflow 用户安装 Python 库
USER airflow
# 核心操作：把 dbt 和 cosmos 装进 Airflow 容器里！
RUN pip install --no-cache-dir dbt-snowflake astronomer-cosmos