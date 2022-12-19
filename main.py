from generator_scripts.accounts_generator import AccountsGenerator
from generator_scripts.transactions_generator import TransactionsGenerator
from transaction_utility import TransactionUtility


if __name__ == '__main__':
    # generate account and assign default balance
    AccountsGenerator()
    # generate transactios and add some constraints
    TransactionsGenerator()
    print("Hello World")
    TransactionUtility.assign_all_account_to_each_shards()
    # print(TransactionUtility.is_account_has_sufficient_amount('27_ZUO',200))