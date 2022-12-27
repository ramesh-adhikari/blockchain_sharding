
from generator_scripts.bootstrap import Bootstrap
from models.transaction import Transaction

if __name__ == '__main__':

    print("Hello World")

    # Bootstrap.run()
    #  storages->shards->01->transactions->confirm/temporary/pool
    # print(Transaction.get_transactions_from_transaction_pool(0))
    # Transaction.move_transaction_from_initial_to_temporary_pool(0,'TXN_1d7242005b207644bfb2d4613d2efb3b900861c2554fcab5cb92a4b731ca35c2')
    # Transaction.move_transaction_from_temporary_to_abort_pool(0,'TXN_1d7242005b207644bfb2d4613d2efb3b900861c2554fcab5cb92a4b731ca35c2')
    # Transaction.remove_transaction_from_temporary_pool(0,'TXN_8b2dae0475134a9c77ef101fda03f18cbcad1b83902636d9604ebf83c3325552')
    # Transaction.append_sub_transaction_to_temporary_file('TXN_7aae3a94427c56d0a029f29f8fcac23c143a25a9fac1c058c30674a17a023321','sub_trxn_1','43_WVJ','ramesh',100)
    # Transaction.move_sub_transaction_to_confirmed_transaction(3,'sub_trxn_1')