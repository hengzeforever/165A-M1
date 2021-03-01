from template.config import *

'''The Page class provides low-level physical storage capabilities. In the provided
skeleton, each page has a fixed size of 4096KB. This should provide optimal
performance when persisting to disk as most hard drives have blocks of the same size.
You can experiment with different sizes. This class is mostly used internally by the
Table class to store and retrieve records. While working with this class keep in mind
that tail and base pages should be identical from the hardware’s point of view.
If your table has 3 columns, you would have a set of base pages containing 3 pages, 1 for each column.

Each column has a physical page, and a base page is a set of such physical pages
each column could have multiple page objects Within the same page range
'''

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)  # Creates an array of that size and initialized with 0

    def has_capacity(self):
        if self.num_records < MAX_NUM_RECORD: 
            return True 
        else:
            return False

    def write(self, value):
        start = self.num_records * INT_SIZE
        end = (self.num_records + 1) * INT_SIZE
        if value != None:
            self.data[start:end] = value.to_bytes(INT_SIZE,'big')
        self.num_records += 1
    
    def len(self):
        return self.num_records

class BasePage:
    
    def __init__(self, num_columns):
        self.colPages = []
        self.num_columns = num_columns
        for i in range(INTERNAL_COL_NUM + self.num_columns):
            self.colPages.append(Page())
            
    def has_capacity(self):
        return self.colPages[0].has_capacity()

class PageRange:
    
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.basePageList = [BasePage(self.num_columns)]
        self.tailPageList = [BasePage(self.num_columns)] # Tail page is essentially the same as base page
        
    def has_capacity(self):
        return len(self.basePageList) < BASE_PAGE_PER_PAGE_RANGE | self.basePageList[-1].has_capacity()

    def create_NewBasePage(self):
        self.basePageList.append(BasePage(self.num_columns))
        return True
        
    def create_NewTailPage(self):
        self.tailPageList.append(BasePage(self.num_columns))
        return True
        
        

