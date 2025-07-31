package simpledb.storage;

import java.util.*;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Permissions;

/**
 * @Description: Implemented to resemble ReadWriteLock in Java
 */
public class LockManager {
    private Map<PageId, Map<TransactionId, Permissions>> locks = new HashMap<>();
    private Map<TransactionId, Set<TransactionId>> waitForGraph = new HashMap<>();

    // acquire lock, block if not available
    public synchronized void acquireLock(PageId pageId, TransactionId tid, Permissions perm)
            throws TransactionAbortedException {
        while (!grantLock(tid, pageId, perm)) {
            Set<TransactionId> blockingTransactions = new HashSet<>();
            Map<TransactionId, Permissions> pageLockMap = locks.get(pageId);

            if (perm != Permissions.READ_ONLY && pageLockMap != null && !pageLockMap.isEmpty()) {
                for (TransactionId t : pageLockMap.keySet()) {
                    if (!t.equals(tid)) {
                        blockingTransactions.add(t);
                    }
                }
            } else if (perm == Permissions.READ_ONLY && pageLockMap != null && !pageLockMap.isEmpty()) {
                for (Map.Entry<TransactionId, Permissions> entry : pageLockMap.entrySet()) {
                    if (!entry.getKey().equals(tid) && entry.getValue() == Permissions.READ_WRITE) {
                        blockingTransactions.add(entry.getKey());
                    }
                }
            }
            if (!blockingTransactions.isEmpty()) {
                waitForGraph.put(tid, blockingTransactions);
                if (hasCycleFrom(tid, waitForGraph)) {
                    waitForGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }

            try {
                wait();
            } catch (InterruptedException e) {
                waitForGraph.remove(tid);
                throw new TransactionAbortedException();
            }
        }
        waitForGraph.remove(tid);
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

    enum Color {
        WHITE, GRAY, BLACK
    } // WHITE=unseen, GRAY=on path, BLACK=done

    public boolean hasCycleFrom(TransactionId start, Map<TransactionId, Set<TransactionId>> waitFor) {
        Map<TransactionId, Color> color = new HashMap<>();

        List<TransactionId> stack = new ArrayList<>();
        List<Iterator<TransactionId>> iteration = new ArrayList<>();

        // Push start node
        stack.add(start);
        iteration.add(neighbors(start, waitFor).iterator());
        color.put(start, Color.GRAY);

        while (!stack.isEmpty()) {
            Iterator<TransactionId> it = iteration.get(iteration.size() - 1);
            if (it.hasNext()) {
                TransactionId nexttransationid = it.next();
                Color c = color.getOrDefault(nexttransationid, Color.WHITE);
                if (c == Color.WHITE) {
                    color.put(nexttransationid, Color.GRAY);
                    stack.add(nexttransationid);
                    iteration.add(neighbors(nexttransationid, waitFor).iterator());
                } else if (c == Color.GRAY) {
                    return true;
                } // BLACK means already finished; ignore
            } else {
                // Done exploring this node
                iteration.remove(iteration.size() - 1);
                TransactionId done = stack.remove(stack.size() - 1);
                color.put(done, Color.BLACK);
            }
        }
        return false;
    }

    private Set<TransactionId> neighbors(TransactionId t,
            Map<TransactionId, Set<TransactionId>> waitFor) {
        return waitFor.getOrDefault(t, Collections.emptySet());
    }

}