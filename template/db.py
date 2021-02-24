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
        pass

    def open(self, workPath):
        if path.exists('database'):
            with open('database', 'rb') as dbfile:
                self.tables = pickle.load(dbfile)   
        else:
            # Do nothing if database file has not been generated
            pass

    def close(self):
        for table in self.tables:
            table.bufferpool.flushDirty()
        with open('database', 'wb') as dbfile:
            pickle.dump(self.tables, dbfile, pickle.HIGHEST_PROTOCOL)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        if name in self.tables:
            return self.get_table(name)
        else:
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
        return self.tables[name]
        
