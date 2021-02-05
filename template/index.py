
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
        
       
        pass


    #hash_table = [None]*1024

    #def Hashing(self,key):
        #return key % len(hash_table)

    def insertIndex(self,column_index,key_val,rid):
        if(self.indices[column_index]== None):
            self.indices[column_index]=dict() #make a dictionary {key val:rid}

        self.indices[column_index][key_val]=rid  
        
    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if self.indices[column]:
           # hash_key = Hashing(column)
            #hash_value = Hashing(value)
            ret = self.indices[column][value]
            return ret

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        ret_val =[]
        for key in self.indices[column]:
            if key>=begin and key<=end:
                ret_val.append(self.indices[column].get(key))
        #print("ret_val=",ret_val)
        return ret_val

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
