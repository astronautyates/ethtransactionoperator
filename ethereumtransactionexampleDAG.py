from datetime import datetime
from airflow import DAG
from EtherscanTransactionOperator import EtherscanTransactionOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 13),
}

dag = DAG('etherscan_operator_example', 
          default_args=default_args, 
          description='An example DAG showing Etherscan operator usage.')

t1 = EtherscanTransactionOperator(
    task_id='get_ethereum_transactions',
    address='0x690B9A9E9aa1C9dB991C7721a92d351Db4FaC990',
    etherscan_api_key='CFZVNFZCHMZN6AKZF4BEM1S8D4PP96RNHH',
    dag=dag,
)
