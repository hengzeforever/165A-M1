# Global Setting for the Database
# PageSize, StartRID, etc..

'''
The config.py file is meant to act as centralized storage for all the configuration options
and the constant values used in the code. It is good practice to organize such
information into a Singleton object accessible from every file in the project. This class
will find more use when implementing persistence in the next milestone.
'''
# Sizes
PAGE_SIZE = 4096
INT_SIZE = 8
MAX_NUM_RECORD = 512
BASE_PAGE_PER_PAGE_RANGE = 16
INTERNAL_COL_NUM = 4
BUFFER_POOL_SIZE = 10

# Indices
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3