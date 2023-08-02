import requests
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class EtherscanTransactionOperator(BaseOperator):
    """
    An Operator to fetch recent transactions from Ethereum blockchain for a given address using Etherscan API.
    """

    @apply_defaults
    def __init__(self, address: str, etherscan_api_key: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.address = address
        self.etherscan_api_key = etherscan_api_key

    def execute(self, context):
        base_url = "https://api.etherscan.io/api"
        response = requests.get(base_url, params={
            'module': 'account',
            'action': 'txlist',
            'address': self.address,
            'startblock': 0,
            'endblock': 99999999,
            'sort': 'asc',
            'apikey': self.etherscan_api_key
        })

        response.raise_for_status()
        transactions = response.json()['result']
        return transactions
