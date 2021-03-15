from template.table import Table
from os import path, chdir, getcwd
import pickle
# Threading not implemented yet

'''
The Database class is a general interface to the database and handles high-level
operations such as starting and shutting down the database instance and loading the
database from stored disk files. This class also handles the creation and deletion of
tables via the create and drop function.The create function will create a new
table in the database. The Table constructor takes as input the name of the table,
number of columns and the index of the key column. The drop function drops the
specified table.
'''

class Database():

    def __init__(self):
        self.tables = {}
        self.oldPath = None
        pass

    def open(self, workPath):
        if path.exists(workPath):
            self.oldPath = getcwd()
            chdir(workPath)
            if path.exists('database'):
                with open('database', 'rb') as dbfile:
                    self.tables = pickle.load(dbfile)

    def close(self):
        for table_name in self.tables:
            self.tables[table_name].lock = None
            self.tables[table_name].lock_manager.lock = None
            self.tables[table_name].bufferpool.flushDirty()
        with open('database', 'wb') as dbfile:
            pickle.dump(self.tables, dbfile, pickle.HIGHEST_PROTOCOL)
        
        chdir(self.oldPath)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        if name in self.tables:
            del self.tables[name]
        table = Table(name, num_columns, key)
        self.tables[name]=table
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        return self.tables.pop(name)
        

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        self.tables[name].continueMerge()
        return self.tables[name]
        
