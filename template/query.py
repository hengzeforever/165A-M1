from template.table import Table, Record
from template.index import Index
from template.page import Page, BasePage, PageRange
from template.config import *
import datetime
'''
The Query class provides standard SQL operations such as insert, select,
update, delete and sum. The select function returns the specified set of columns
from the record with the given key (if available). The insert function will insert a new
record in the table. All columns should be passed a non-NULL value when inserting. The
update function updates values for the specified set of columns. The delete function
will delete the record with the specified key from the table. The sum function will sum
over the values of the selected column for a range of records specified by their key
values. We query tables by direct function calls rather than parsing SQL queries.
'''

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass
    
    # Get the newest column values for a base record 
    def getNewestColumns(self, baseRID):
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
        #return newsetColumns -> [92106429,g1,g2,g3,g4]
    """
    # internal Method
    # Read a record with specified key
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # Get base record info
        baseRID = self.table.keyToBaseRID[primary_key]
        baseLocation = self.table.baseRIDToLocation(baseRID)
        baseRecord = self.table.baseRIDToRecord(baseRID)
        baseIndirect = baseRecord[INDIRECTION_COLUMN]

        # Set baseRID to zero
        self.table.baseWriteByte(0, baseLocation, RID_COLUMN)
        
        # Set all associated tailRID to zero
        if (baseIndirect != 0):
            tailRID = baseIndirect
            tailRecord = self.table.tailRIDToRecord(tailRID)
            tailIndirect = tailRecord[INDIRECTION_COLUMN]
            while tailIndirect != 0:
                tailLocaiton = self.table.tailPage_lib[tailRID]
                self.table.tailWriteByte(0, tailLocaiton, RID_COLUMN)
                tailRID = tailIndirect
                tailRecord = self.table.tailRIDToRecord(tailRID)
                tailIndirect = tailRecord[INDIRECTION_COLUMN]
            tailLocaiton = self.table.tailPage_lib[tailRID]
            self.table.tailWriteByte(0, tailLocaiton, RID_COLUMN)
        return True


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        '''record example:[0, 0, 20210131111207, 0, 906659671, 93, 0, 0, 0]'''
        # Check if key is duplicated
        if columns[self.table.key] in self.table.keyToBaseRID:
            return False

        # Initialize columns for base record
        baseIndirect = 0
        baseRID = self.table.baseRID
        time = datetime.datetime.now()
        schema_encoding = "0" * self.table.num_columns
        columns = list(columns)
        
        # Create shorter names
        curPageRange = self.table.pageRanges[-1]
        curBasePage = curPageRange.basePageList[-1]

        # Open a new page range or new base page if there isn't enough space
        if curPageRange.has_capacity() == False:
            self.table.create_NewPageRange()
            curPageRange = self.table.pageRanges[-1]
            curBasePage = curPageRange.basePageList[-1]
        elif curBasePage.has_capacity() == False:
            curPageRange.create_NewBasePage()
            curBasePage = curPageRange.basePageList[-1]

        # Write the base record into basepage
        baseRecord = [baseIndirect, baseRID, int(time.strftime("%Y%m%d%H%M%S")), int(schema_encoding, 2)] + columns
        for i in range(len(baseRecord)):
            curBasePage.basePage[i].write(baseRecord[i])
        
        # Update table's private variables
        self.table.keyToBaseRID[baseRecord[self.table.key + INTERNAL_COL_NUM]] = baseRID
        self.table.index.insertIndex(self.table.key,columns[self.table.key],baseRID)

        self.table.baseRID += 1
        return True
    
    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, index_key, column, query_columns):
        listSelect = []
        recordSelect = []
        if column == self.table.key: #search by primary key column
            baseRID = self.table.index.locate(column,index_key)
            newestColumns = self.getNewestColumns(baseRID)
            for i in range(len(query_columns)):
                if query_columns[i] == 0:
                    recordSelect.append(None)
                else:
                    recordSelect.append(newestColumns[i])
            listSelect.append(Record(baseRID, index_key, recordSelect))
            return listSelect
        else: #searching by other column
            self.table.index.create_index(column)
            baseRID = self.table.index.locate(column,index_key)
            newestColumns=[]
            for count,baserid in enumerate(baseRID):
                newestColumns.append(self.getNewestColumns(baserid))
                for i in range(len(query_columns)):
                    if query_columns[i] == 0:
                        recordSelect.append(None)
                    else:
                        recordSelect.append(newestColumns[count][i])
                listSelect.append(Record(baserid, index_key, recordSelect))
            return listSelect
    """             
        listSelect = []
        recordSelect = []
        if column == self.table.key: #searching by primary key
            baseRID = self.table.keyToBaseRID[index_key]
            newestColumns = self.getNewestColumns(baseRID)
            for i in range(len(query_columns)):
                if query_columns[i] == 0:
                    recordSelect.append(None)
                else:
                    recordSelect.append(newestColumns[i])
            listSelect.append(Record(baseRID, index_key, recordSelect))
            return listSelect
        else: #searching by other column
            self.table.index.create_index(column)
            baseRID = self.table.index.locate(column,index_key)
            if 
            for count,baserid in enumerate(baseRID):
                newestColumns=[]
                newestColumns.append(self.getNewestColumns(baserid))
                for i in range(len(query_columns)):
                    if query_columns[i] == 0:
                        recordSelect.append(None)
                    else:
                        recordSelect.append(newestColumns[count][i])
                listSelect.append(Record(baseRID, index_key, recordSelect))
            return listSelect
    """
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    
    def update(self, primary_key, *columns):
        # Get associated base record info
        baseRID = self.table.keyToBaseRID[primary_key]
        baseLocation = self.table.baseRIDToLocation(baseRID)
        pageRange_index = baseLocation[0]
        baseRecord = self.table.baseRIDToRecord(baseRID)

        # Initialize columns for tailRecord
        tailIndirect = 0
        tailRID = self.table.tailRID
        time = datetime.datetime.now()
        updateEncoding = ""
        columns = list(columns)
        '''
        print('\n')
        print("Before update:", baseRecord, "columns:", columns)
        '''
        # Create shorter names
        curPageRange = self.table.pageRanges[pageRange_index]
        curTailPage = curPageRange.tailPageList[-1]

        # Open a new tail page if there isn't enough space
        if curPageRange.tailPageList[-1].has_capacity() == False:
            #merge when one tailPage full
            self.table.merge()
            curPageRange.create_NewTailPage()
            curTailPage = curPageRange.tailPageList[-1]
        
        # Prepare values for tail record
        baseRecordIndirect = baseRecord[INDIRECTION_COLUMN]
        if baseRecordIndirect != 0:
            # Current tail record is not the first update to the base record
            lastTailRID = baseRecordIndirect
            tailIndirect = lastTailRID
            lastTailRecord = self.table.tailRIDToRecord(lastTailRID)

            # Cumulate schema encoding for tail record based on the last tail record
            binaryLastScheme = bin(lastTailRecord[SCHEMA_ENCODING_COLUMN])[2:]
            lastScheme = "0" * (len(columns)-len(binaryLastScheme)) + binaryLastScheme
            for i in range(len(columns)):
                if (columns[i] == None) & (lastScheme[i] == "0"):
                    updateEncoding += "0"
                else:
                    updateEncoding += "1"

            # Modify column values based on schema encoding
            lastColumns = lastTailRecord[4:]
            for i in range(len(columns)):
                if updateEncoding[i] != "0":
                    if columns[i] == None:
                        columns[i] = lastColumns[i]
        else:
            # Current tail record is the first update to a base record
            for i in range(len(columns)):
                if columns[i] == None:
                    updateEncoding += "0"
                else:
                    updateEncoding += "1"

        # Write tail record into tail page
        tailRecord = [tailIndirect, tailRID, int(time.strftime("%Y%m%d%H%M%S")), int(updateEncoding, 2)] + columns
        for i in range(len(tailRecord)):
            curTailPage.basePage[i].write(tailRecord[i])
        
        # Update base record's indirection column and schema encoding column
        self.table.baseWriteByte(tailRID, baseLocation, INDIRECTION_COLUMN)
        self.table.baseWriteByte(int(updateEncoding, 2), baseLocation, SCHEMA_ENCODING_COLUMN)
        '''
        # Update columns values for base record
        for i in range(len(columns)):
            if updateEncoding[i] != "0":
                self.table.baseWriteByte(columns[i], baseLocation, i+4) 
        '''
        # Update table's private variables
        tailPageList_index = len(curPageRange.tailPageList) - 1
        offset_index = curTailPage.basePage[0].len() - 1
        self.table.tailPage_lib[tailRID] = [pageRange_index, tailPageList_index, offset_index]
        self.table.tailRID += 1

        #Update index 2/22
        for col_num, col in enumerate(columns):
            if col != None: 
                self.table.index.indices[col_num][col]=baseRID
            
        '''
        baseRecord = self.table.baseRIDToRecord(baseRID)
        #print("After update:", baseRecord, "columns:", columns,'\n')
        '''
        return True
        
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        listrids = self.table.index.locate_range(start_range,end_range, self.table.key)
        sum = 0
        if len(listrids) == 0:
            return False
        else:
            for rid in listrids:
                newestColumns = self.getNewestColumns(rid)
                sum += newestColumns[aggregate_column_index]
            return sum

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False