from template.page import PageRange
from template.index import Index
from template.config import *
from template.bufferpool import BufferPool
from copy import deepcopy
import threading
import time
'''
The Table class provides the core of our relational storage functionality. All columns are
64-bit integers in this implementation. Users mainly interact with tables through queries.
Tables provide a logical view over the actual physically stored data and mostly manage
the storage and retrieval of data. Each table is responsible for managing its pages and
requires an internal page directory that given a RID it returns the actual physical location
of the record. The table class should also manage the periodical merge of its
corresponding page ranges.
'''

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
    
    def getColumns(self):
        return self.columns
'''
class BaseRecord:

    def __init__(self, recordData, basePage, location):
        self.recordData = recordData # a list of integers
        self.basePage = basePage
        self.location = location

    def getLocation(self):
        return self.location
    
    def setLocation(self, location):
        self.location = location

    def getRecordData(self):
        return self.getRecordData
    
    def setRecordData(self, recordData):
        self.recordData = recordData

    def getBasePage(self):
        
        return self.basePage
    
    def setBasePage(self, basePage):
        self.basePage = basePage

class PageDirectory:

    def __init__(self):
        self.pageDict = {} # key:baseRID val:[location, basePage]

    def insertRecord(self, baseRID, baseRecord):
        self.pageDict[baseRID] = baseRecord
    
    def getRecord(self, baseRID):
        return self.pageDict[baseRID]
    
    def setRecord(self, baseRID):
    
    def change
    

    # Given a baseRID return a list of values in base record
    # The way to access a value using a location: 
    # e.g. value = int.from_bytes(pageRanges[pageRange_index].basePageList
    # [basePageList_index].colPages[columnNum].data[offset_index*INT_SIZE:(offset_index+1)*INT_SIZE], 'big')
    def baseRIDToRecord(self, baseRID):
        location = self.pageDict[baseRID].getLocation()
        pageRange_index = location[0]
        basePageList_index = location[1]
        offset_index = location[2]
        
        bufferPageRange = self.bufferpool.getPageRange(pageRange_index)
        baseRecord = []
        for i in range(INTERNAL_COL_NUM+self.num_columns):
            baseRecord.append(int.from_bytes(bufferPageRange.pageRange.basePageList[basePageList_index].colPages[i].data \
                [offset_index*INT_SIZE:(offset_index+1)*INT_SIZE], 'big'))
            
        return baseRecord'''

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.bufferpool = BufferPool(self.num_columns)
        #self.page_directory = {}
        self.basePage_dir = {}
        self.tailPage_dir = {} # Store tailRID: tailLocation, so that we can find a tail record
        self.tailRIDTOBaseRID = {}
        self.index = Index(self)
        self.num_PageRanges = 1

        # baseRID and tailRID are initialized to 1, 0 is for deleted record
        self.baseRID = 1
        self.tailRID = 1

        #merge
        self.mergeQ = []
        #self.deallocateQ = []
        self.mergedCount = 0

        thread = threading.Thread(target=self.merge, args=())
        thread.daemon = True
        thread.start()

    def create_NewPageRange(self):
        emptyPageRange = self.bufferpool.getEmptyPage()
        emptyPageRange.pageRange_Index = self.num_PageRanges
        emptyPageRange.pageRange = PageRange(self.num_columns)
        self.num_PageRanges += 1
        return True

    # Given a baseRID return a list of values in base record
    # The way to access a value using a location: 
    # e.g. value = int.from_bytes(pageRanges[pageRange_index].basePageList
    # [basePageList_index].colPages[columnNum].data[offset_index*INT_SIZE:(offset_index+1)*INT_SIZE], 'big')
    def baseRIDToRecord(self, baseRID):
        location = self.basePage_dir[baseRID]
        pageRange_index = location[0]
        basePageList_index = location[1]
        offset_index = location[2]
        
        bufferPageRange = self.bufferpool.getPageRange(pageRange_index)
        baseRecord = []
        for i in range(INTERNAL_COL_NUM+self.num_columns):
            baseRecord.append(int.from_bytes(bufferPageRange.pageRange.basePageList[basePageList_index].colPages[i].data \
                [offset_index*INT_SIZE:(offset_index+1)*INT_SIZE], 'big'))
            
        return baseRecord

    
    # Given a tailRID return a list of values in tail record
    def tailRIDToRecord(self, tailRID):
        location = self.tailPage_dir[tailRID]
        pageRange_index = location[0]
        tailPageList_index = location[1]
        offset_index = location[2]

        bufferPageRange = self.bufferpool.getPageRange(pageRange_index)
        tailRecord = []
        for i in range(INTERNAL_COL_NUM+self.num_columns):
            tailRecord.append(int.from_bytes(bufferPageRange.pageRange.tailPageList[tailPageList_index].colPages[i].data \
                [offset_index*INT_SIZE:(offset_index+1)*INT_SIZE], 'big'))
        return tailRecord
    
    # Overwrite a value in base record
    def baseWriteByte(self, value, baseRID, columnNum):
        location = self.basePage_dir[baseRID]
        pageRange_index = location[0]
        basePageList_index = location[1]
        offset_index = location[2]

        bufferPageRange = self.bufferpool.getPageRange(pageRange_index)
        bufferPageRange.pageRange.basePageList[basePageList_index] \
            .colPages[columnNum].data[offset_index*INT_SIZE:(offset_index+1)*INT_SIZE] = \
                value.to_bytes(INT_SIZE, 'big')
        return True

    # Overwrite a value in tail record
    def tailWriteByte(self, value, tailRID, columnNum):
        location = self.tailPage_dir[tailRID]
        pageRange_index = location[0]
        tailPageList_index = location[1]
        offset_index = location[2]

        bufferPageRange = self.bufferpool.getPageRange(pageRange_index)
        bufferPageRange.pageRange.tailPageList[tailPageList_index] \
            .colPages[columnNum].data[offset_index*INT_SIZE:(offset_index+1)*INT_SIZE] = \
                value.to_bytes(INT_SIZE, 'big')
        return True

    # Commit available associated tail page
    def commitTailPage(self, tailPage):
        self.mergeQ.append(tailPage)

    def merge(self):
        #/// merge: operating on base record
        #/// get the update tail record, lock the base record and apply in place update, then release the lock
        while True:
            if self.mergeQ != []:
                tailPage = self.mergeQ.pop(0)
                lastestApplied = {}
                for offset_index in range(512):
                    # reverse iteration
                    tailRID = int.from_bytes(tailPage.colPages[RID_COLUMN].data[(511-offset_index)*INT_SIZE:(511-offset_index+1)*INT_SIZE], 'big')
                    baseRID = self.tailRIDTOBaseRID[tailRID]
                    if not baseRID in lastestApplied:
                        lastestApplied[baseRID] = tailRID
                        tailRecord = self.tailRIDToRecord(tailRID)
                        baseRecord = self.baseRIDToRecord(baseRID)
                        binarySchema = bin(baseRecord[SCHEMA_ENCODING_COLUMN])[2:]
                        schema_encoding = "0" * (self.num_columns-len(binarySchema)) + binarySchema
                        for i in range(self.num_columns):
                            if schema_encoding[i] != "0":
                                self.baseWriteByte(tailRecord[i+INTERNAL_COL_NUM], baseRID, i+INTERNAL_COL_NUM)
                self.mergedCount += 1
            #print("merged count:", self.mergedCount)
            time.sleep(1)


    def continueMerge(self):
        thread = threading.Thread(target=self.merge, args=())
        thread.daemon = True
        thread.start()


                

                    
