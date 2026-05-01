from pathlib import Path
from datetime import datetime
from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig

# 1. 告诉 Cosmos 你的 dbt 项目在容器里的绝对路径
# 因为你把它放在了 dags/dbt/ 下，Airflow 挂载进容器后，路径默认就是下面这个：
DBT_PROJECT_PATH = Path("/opt/airflow/dags/dbt/web3_analytics")

# 2. 配置 Snowflake 认证信息 (让它去读你塞进去的 profiles.yml)
profile_config = ProfileConfig(
    profile_name="web3_analytics",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml"
)

# 3. 见证魔法：只需一个 DbtDag 声明，Cosmos 会自动解析里面所有的 SQL 并画出图表！
web3_cosmos_dag = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    operator_args={"install_deps": True},  # 自动安装 dbt 依赖包
    profile_config=profile_config,
    # 告诉 Cosmos dbt 命令在哪里执行 (这是 Docker 安装的默认路径)
    execution_config=ExecutionConfig(dbt_executable_path="/home/airflow/.local/bin/dbt"),

    # 基础 DAG 配置
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 30),
    catchup=False,
    dag_id="cosmos_web3_smart_money_pipeline",  # 新的名字！
    tags=["web3", "cosmos", "dbt_magic"]
)