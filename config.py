from multiprocessing import Value

SHARDS = [(0, True), (1, False),
          (2, False), (3, True),
          (4, False), (5, True)
          ]  # (id,is_leader)

TOTAL_NUMBER_OF_TRANSACTIONS = 100
NUMBER_OF_ACCOUNTS = 1000
DEFAULT_AMOUNT = 2000
NUMBER_OF_CONDITIONS = 4
TRANSACTION_TYPE = 'LOCK'  # NO_LOCK, LOCK, OUR_PROTOCOL
ACCOUNT_LOCK_RETRY_TIME_MS = 1000
WRITE_LOG_TO_FILE = False

LOCK_FILE_TO_MAKE_THREAD_SAFE = True

CONDITION_HAS = '__HAS__'
CONDITION_AND = '____AND____'
TRANSACTION_FILE_NAME = 'TRANSACTIONS.CSV'
ACCOUNT_INDEX_ACCOUNT_NUMBER = 0
ACCOUNT_INDEX_ACCOUNT_NAME = 1
ACCOUNT_INDEX_AMOUNT = 2

TRANSACTION_STATE_INITIAL = 'INITIAL'
TRANSACTION_STATE_COMMITTED = 'COMMITTED'
TRANSACTION_STATE_ABORTED = 'ABORTED'
TRANSACTION_STATE_ROLLBACKED = 'ROLLBACKED'
SNAPSHOT_ACQUIRED = 'ACQUIRED'
SNAPSHOT_RELEASED = 'RELEASED'

LOCK_LOCKED = 'LOCKED'
LOCK_RELEASED = 'RELEASED'


MESSAGE_SEPARATOR = "***"
MESSAGE_DATA_SEPARATOR = "___"

HOST = '127.0.0.1'
INITAIL_PORT = 8085
