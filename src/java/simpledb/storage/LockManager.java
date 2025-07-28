package simpledb.storage;

import java.util.*;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Permissions;;

/**
 * @Description: Implemented to resemble ReadWriteLock in Java
 */
public class LockManager {
    private Map<PageId, Map<TransactionId, Permissions>> locks = new HashMap<>();

    // acquire lock, block if not available
    public synchronized void acquireLock(PageId pageId, TransactionId tid, Permissions perm)
            throws TransactionAbortedException {
        while (!grantLock(tid, pageId, perm)) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new TransactionAbortedException();
            }
        }

        locks.computeIfAbsent(pageId, _ -> new HashMap<>()).put(tid, perm);
    }

    // check if the lock can be granted
    private boolean grantLock(TransactionId tid, PageId pageId, Permissions perm) {
        Map<TransactionId, Permissions> pageLocks = locks.get(pageId);
        if (pageLocks == null || pageLocks.isEmpty())
            return true;

        // if only this transaction holds the lock
        if (pageLocks.size() == 1 && pageLocks.containsKey(tid)) {
            // Allow reacquiring, upgrading (read -> write) or downgrading (wrte -> read)
            // the lock
            return true;
        }

        // Shared lock: allow multiple READ_ONLY (something like the ReadWriteLock)
        if (perm == Permissions.READ_ONLY) {
            for (Permissions p : pageLocks.values()) {
                if (p == Permissions.READ_WRITE)
                    return false;
            }
            return true;
        }

        // exclusive lock: only if no other locks or if transaction is the only one
        // holding a shared lock on that object
        // Upgrade shared lock to exclusive lock
        return false;
    }

    // release lock
    public synchronized void releaseLock(PageId pageId, TransactionId tid) {
        Map<TransactionId, Permissions> pageLocks = locks.get(pageId);
        if (pageLocks != null) {
            pageLocks.remove(tid);
            if (pageLocks.isEmpty())
                locks.remove(pageId);
            notifyAll(); // notify waiting threads that a lock has been released
        }
    }

    // check if a transaction holds a lock on a page
    public boolean holdLock(PageId pageId, TransactionId tid) {
        Map<TransactionId, Permissions> pageLocks = locks.get(pageId);
        return pageLocks != null && pageLocks.containsKey(tid);
    }

    // Helper method used in BufferPool evictPage
    // Check if a page is holding to any lock used in a transaction
    public boolean isHoldingLock(PageId pageId) {
        Set<TransactionId> tids = locks.get(pageId).keySet();
        return tids != null && !tids.isEmpty();
    }

    // Helper method used in BufferPool to get all pages holding onto lock for this
    // transaction
    public Set<PageId> getPagesLockedBy(TransactionId tid) {
        // Use set for unordered data, hash set to avoid duplicate page ids
        Set<PageId> lockedPageIds = new HashSet<>();
        for (Map.Entry<PageId, Map<TransactionId, Permissions>> entry : locks.entrySet()) {
            if (entry.getValue().containsKey(tid)) {
                lockedPageIds.add(entry.getKey());
            }
        }
        return lockedPageIds;
    }
}
