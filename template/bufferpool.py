from template.config import *
import pickle
"""
BufferPageRange is a page range that stays on BufferPool.
"""
class BufferPageRange:

    def __init__(self):
        self.pageRange_Index = None
        self.pageRange = None
        self.pin = 0
        self.dirty = 0
    
    def isEmpty(self):
        return self.pageRange_Index == None

    def isDirty(self):
        return self.dirty == 1
    
    # Check if there is no transcation on this page range.
    def isAvailable(self):
        return self.pin == 0
    
    def readFromFile(self):
        with open('pagerange%d' %self.pageRange_Index, 'rb') as file:
            self.pageRange = pickle.load(file)
        self.dirty = 0
        return True

    def writeToFile(self):
        with open('pagerange%d' %self.pageRange_Index, 'wb') as file:
            pickle.dump(self.pageRange, file, pickle.HIGHEST_PROTOCOL)
        self.dirty = 0
        return True

    def clear(self):
        self.pageRange_Index = None
        self.pageRange = None
        self.pin = 0
        self.dirty = 0
        return True
    

"""
BufferPool is a container that holds several BufferPageRanges
"""

class BufferPool:

    def __init__(self):
        self.pageRanges = [BufferPageRange()] * BUFFER_POOL_SIZE
    
    def isFull(self):
        for pageRange in self.pageRanges:
            if pageRange.isEmpty():
                return False
        return True

    # Flush all dirty page ranges before closing database
    def flushDirty(self):
        for pageRange in self.pageRanges:
            if pageRange.isDirty():
                pageRange.writeToFile()
        return True

    # Evict the least recently used page range(Could use MRU as well)//////////LRU unimplemented!
    def evict(self):
        for pageRange in self.pageRanges:
            if pageRange.isAvailable:
                if pageRange.isDirty:
                    pageRange.writeToFile()
                pageRange.clear()
                return True
        return True

    # Load the wanted page range onto buffer pool
    def load(self, pageRange_index):
        if self.isFull():
            self.evict()
        for pageRange in self.pageRanges:
            if pageRange.isEmpty():
                pageRange.pageRange_Index = pageRange_index
                pageRange.readFromFile()
                return True

    # +++ member functions in table class, similar here?
