from multiprocessing import Value

SHARDS = [(0, True), (1, False), (2, False),
          (3, True), (4, True), (5, False), (6, False)]  # (id,is_leader)

TOTAL_NUMBER_OF_TRANSACTIONS = 30
NUMBER_OF_ACCOUNTS = 10
DEFAULT_AMOUNT = 2000
NUMBER_OF_CONDITIONS = 4
TRANSACTION_TYPE = 'OUR_PROTOCOL'  # NO_LOCK, LOCK, OUR_PROTOCOL
ACCOUNT_LOCK_RETRY_TIME_MS = 100
WRITE_LOG_TO_FILE = True
LOCK_FILE_TO_MAKE_THREAD_SAFE = True

CONDITION_HAS = '__HAS__'
CONDITION_AND = '____AND____'
TRANSACTION_FILE_NAME = 'TRANSACTIONS.CSV'
ACCOUNT_INDEX_ACCOUNT_NUMBER = 0
ACCOUNT_INDEX_ACCOUNT_NAME = 1
ACCOUNT_INDEX_AMOUNT = 2


MESSAGE_SEPARATOR = "***"
MESSAGE_DATA_SEPARATOR = "___"

HOST = '127.0.0.1'
INITAIL_PORT = 8085
