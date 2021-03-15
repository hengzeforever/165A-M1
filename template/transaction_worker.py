from template.table import Table, Record
from template.index import Index
import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self):
        self.stats = []
        self.transactions = []
        self.result = 0
        self.thread = threading.Thread(target=self.runThread)
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs a transaction
    """
    def runThread(self):
        i = 0
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
            i += 1
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

    def run(self):
        self.thread.start()

    def join(self):
        self.thread.join()

