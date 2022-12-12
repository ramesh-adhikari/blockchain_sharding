from generator_scripts.accounts_generator import Accounts_Generator
from generator_scripts.transactions_generator import Transactions_Generator


if __name__ == '__main__':
    # generate account and assign default balance
    Accounts_Generator()
    # generate transactios and add some constraints
    Transactions_Generator()
    print("Hello World")