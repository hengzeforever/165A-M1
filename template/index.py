from template.config import *
import time
"""
A data strucutre holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.

The Index class provides a data structure that allows fast processing of queries (e.g.,
select or update) by indexing columns of tables over their values. Given a certain
value for a column, the index should efficiently locate all records having that value. The
key column of all tables is usually indexed by default for performance reasons.
Supporting indexing is optional for this milestone. The API for this class exposes the
two functions create_index and drop_index (optional for this milestone).
"""

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table
        self.indices[self.table.key] = dict()
        pass

    def insertIndex(self, columns, rid):
        for columnNum in range(self.table.num_columns):
            if self.indices[columnNum] != None:
                columVal = columns[columnNum]
                if columVal in self.indices[columnNum]:
                    self.indices[columnNum][columVal].append(rid)
                else:
                    self.indices[columnNum][columVal] = [rid]

    def updateIndex(self, newColumns, lastColumns, rid):
        for columnNum in range(self.table.num_columns):
            oldVal = lastColumns[columnNum]
            newVal = newColumns[columnNum]
            if self.indices[columnNum] != None and newVal != None:
                if newVal != oldVal:
                    if oldVal in self.indices[columnNum]:
                        if rid in self.indices[columnNum][oldVal]:
                            self.indices[columnNum][oldVal].remove(rid)
                        else:
                            time.sleep(0.001)
                    if newVal in self.indices[columnNum]:
                        self.indices[columnNum][newVal].append(rid)
                    else:
                        self.indices[columnNum][newVal] = [rid]

    def removeIndex(self, primary_key):
        del self.indices[self.table.key][primary_key]
    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if self.indices[column]:
            ret = self.indices[column][value]
            return ret
        return False
        

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        ret_val =[]
        for key in self.indices[column]:
            if key>=begin and key<=end:
                ret_val.append(self.indices[column].get(key))
        return ret_val

    """
    # optional: Create index on specific column
    """
    
    def create_index(self, column_number):
        if(self.indices[column_number]== None):
            self.indices[column_number]=dict()
        return True

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number]=None
