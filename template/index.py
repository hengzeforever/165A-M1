from template.config import *
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
            return ret #can return a list of rid or just one rid

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
    def getNewestColumns(self, baseRID): #from query.py
        newestColumns = []
        baseRecord = self.table.baseRIDToRecord(baseRID)
        baseIndirect = baseRecord[INDIRECTION_COLUMN]

        if baseIndirect == 0:
            # Base record has no update
            newestColumns = baseRecord[4:]
        else:
            # Get the latest update
            tailRID = baseIndirect
            tailRecord = self.table.tailRIDToRecord(tailRID)
            binarySchema = bin(baseRecord[SCHEMA_ENCODING_COLUMN])[2:]
            schema_encoding = "0" * (self.table.num_columns-len(binarySchema)) + binarySchema
            for i in range(self.table.num_columns):
                if schema_encoding[i] == "1":
                    val = tailRecord[i+INTERNAL_COL_NUM]
                else:
                    val = baseRecord[i+INTERNAL_COL_NUM]
                newestColumns.append(val)
        return newestColumns
    
    def create_index(self, column_number):
        val_with_rid = {} #dictionary of val:list_of_rid
        #loop through all baserecord we have, find the value of that column, add val and rid in dict()
        for rid in range(1,self.table.baseRID+1):
            record = self.getNewestColumns(rid)
            val= record[column_number]
            if val in val_with_rid: #if val already exist in the dict
                val_with_rid[val].append(rid)
            else:
                val_with_rid[val]=[rid]
        #if only one rid match with a val:
        #if len(val_with_rid[val])==0:
            #val_with_rid[val]=val_with_rid[val][0] #make it val:rid
        self.indices[column_number]= val_with_rid
        

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number]=None
