
from generator_scripts.bootstrap import Bootstrap
from models.transaction import Transaction

if __name__ == '__main__':

    print("Hello World")

    # Bootstrap.run()
    #  storages->shards->01->transactions->confirm/temporary/pool
    # print(Transaction.get_transactions_from_transaction_pool(0))
    # Transaction.move_transaction_from_initial_to_temporary_pool(0,'TXN_1d7242005b207644bfb2d4613d2efb3b900861c2554fcab5cb92a4b731ca35c2')
    # Transaction.move_transaction_from_temporary_to_abort_pool(0,'TXN_fadb11b575c8f48c6b8a3be49086eda55f3fb7670d24e922c7653f9946fdccb3')
    # Transaction.remove_transaction_from_temporary_pool(0,'TXN_8b2dae0475134a9c77ef101fda03f18cbcad1b83902636d9604ebf83c3325552')
    # Transaction.append_sub_transaction_to_temporary_file('TXN_7aae3a94427c56d0a029f29f8fcac23c143a25a9fac1c058c30674a17a023321','sub_trxn_1','43_WVJ','ramesh',100)
    # Transaction.move_sub_transaction_to_confirmed_transaction(3,'sub_trxn_1')
    # Transaction.move_transaction_from_temporary_to_initial('0','TXN_48b703d3f1eb8737a517f73fe7e4a52a90a71a7bdf017b37b98995c856330b27')