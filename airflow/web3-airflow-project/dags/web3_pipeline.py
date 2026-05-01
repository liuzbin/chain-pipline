from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 定义默认参数
default_args = {
    'owner': 'zhibin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# 实例化 DAG
with DAG(
    'web3_smart_money_pipeline',
    default_args=default_args,
    description='以太坊巨鲸追踪每日全链路流水线',
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 29), # 设为昨天，这样它等会儿会立刻补跑一次
    catchup=False,
    tags=['web3', 'dbt', 'spark'],
) as dag:

    # 节点 1：模拟 Spark 往 S3 写入数据 (假装需要 5 秒)
    ingest_raw_data = BashOperator(
        task_id='spark_ingest_to_s3',
        bash_command='echo "🚀 Spark 开始消费 Kafka 并写入 S3..." && sleep 5 && echo "✅ 写入完成！"'
    )

    # 节点 2：模拟 dbt 跑 Silver 层清洗 (假装需要 3 秒)
    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver_layer',
        bash_command='echo "🧹 dbt 正在清洗 raw 数据为 stg 表..." && sleep 3 && echo "✅ 清洗完成！"'
    )

    # 节点 3：模拟 dbt 跑数据测试 (这步极快)
    dbt_test_silver = BashOperator(
        task_id='dbt_test_silver_layer',
        bash_command='echo "🧪 dbt 正在执行 not_null 和 accepted_values 测试..." && sleep 2 && echo "✅ 测试全绿通过！"'
    )

    # 节点 4：模拟 dbt 跑 Gold 层巨鲸汇总
    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold_layer',
        bash_command='echo "👑 dbt 正在计算 smart_money 巨鲸榜单..." && sleep 4 && echo "✅ 金层数据就绪！"'
    )

    # 🌟 依赖绑定：见证奇迹的位移运算符
    ingest_raw_data >> dbt_run_silver >> dbt_test_silver >> dbt_run_gold