import threading

class LockManager:
    def __init__(self):
        self.shared_lock ={} # for read  {rid:readerCounts}
        self.exclusive_lock=[] #for write [rid1,rid2...]
        self.lock = threading.Lock() # for protecting lock mangager resources

    # check if we can acquire, reture true or false
    def acquire(self, LockType, rid):
        self.lock.acquire()
        if rid in self.exclusive_lock:
            self.lock.release()
            return False
        else:
            if LockType == 'R':
                if rid in self.shared_lock: # this rid is accessed by other readers
                    self.shared_lock[rid] += 1
                else:
                    self.shared_lock[rid] = 1
            elif LockType =='W':
                if rid in self.shared_lock:
                    self.lock.release()
                    return False
                else:
                    self.exclusive_lock.append(rid)
        self.lock.release()
        return True

    # return true or false
    def release(self, LockType, rid):
        self.lock.acquire()
        if LockType == 'R':
            if rid in self.shared_lock:
                if self.shared_lock[rid] == 1:
                    del self.shared_lock[rid]
                else:
                    self.shared_lock[rid] -= 1
            else:
                self.lock.release()
                return False
        elif LockType == 'W':
            if rid in self.exclusive_lock:
                self.exclusive_lock.remove(rid)
            else:
                self.lock.release()
                return False
        self.lock.release()
        return True