
from generator_scripts.bootstrap import Bootstrap
from models.transaction import Transaction

if __name__ == '__main__':

    print("Hello World")

    Bootstrap.run()
    #  storages->shards->01->transactions->confirm/temporary/pool
