package simpledb.storage;

import java.util.*;

import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Permissions;

/**
 * @Description: Implemented to resemble ReadWriteLock in Java
 */
public class LockManager {
    private Map<PageId, Map<TransactionId, Permissions>> locks = new HashMap<>();
    private Map<TransactionId, Set<TransactionId>> cycleGraph = new HashMap<>();

    // acquire lock, block if not available
    public synchronized void acquireLock(PageId pageId, TransactionId tid, Permissions perm)
            throws TransactionAbortedException {
        while (!grantLock(tid, pageId, perm)) {

            Set<TransactionId> blockers = getBlockers(pageId, tid,perm);

            cycleGraph.put(tid, new HashSet<>(blockers));

            if (detectCycleFrom(tid)) {
                // Cycle detected, abort the transaction
                removeAllEdgesOf(tid);
                throw new TransactionAbortedException();
            }

            try {
                wait();
            } catch (InterruptedException e) {
                removeAllEdgesOf(tid);
                throw new TransactionAbortedException();
            } finally {
                // Clean up the cycle graph for this transaction
                cycleGraph.remove(tid);
            }
        }
        cycleGraph.remove(tid);
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
            // notify waiting threads that a lock has been released
        }
        removeAllEdgesOf(tid);
        notifyAll(); 
    }

    // check if a transaction holds a lock on a page
    public synchronized boolean holdLock(PageId pageId, TransactionId tid) {
        Map<TransactionId, Permissions> pageLocks = locks.get(pageId);
        return pageLocks != null && pageLocks.containsKey(tid);
    }

    // Helper method used in BufferPool to get all pages holding onto lock for this
    // transaction
    public synchronized Set<PageId> getPagesLockedBy(TransactionId tid) {
        // Use set for unordered data, hash set to avoid duplicate page ids
        Set<PageId> lockedPageIds = new HashSet<>();
        for (Map.Entry<PageId, Map<TransactionId, Permissions>> entry : locks.entrySet()) {
            if (entry.getValue().containsKey(tid)) {
                lockedPageIds.add(entry.getKey());
            }
        }
        return lockedPageIds;
    }

    // private Set<TransactionId> getBlockers(PageId pageId, TransactionId tid) {
    // Set<TransactionId> blockers = new HashSet<>();
    // Map<TransactionId, Permissions> pageLocks = locks.get(pageId);
    // if (pageLocks != null) {
    // for (Map.Entry<TransactionId, Permissions> entry : pageLocks.entrySet()) {
    // if (!entry.getKey().equals(tid)) {
    // blockers.add(entry.getKey());
    // }
    // }
    // }
    // return blockers;
    // }

    private Set<TransactionId> getBlockers(PageId pageId, TransactionId tid, Permissions perm) {
        Map<TransactionId, Permissions> pageLocks = locks.get(pageId);
        if (pageLocks == null || pageLocks.isEmpty())
            return Collections.emptySet();

        Set<TransactionId> blockers = new HashSet<>();
        boolean iHold = pageLocks.containsKey(tid); // for upgrade case

        for (Map.Entry<TransactionId, Permissions> e : pageLocks.entrySet()) {
            TransactionId owner = e.getKey();
            if (owner.equals(tid))
                continue;

            Permissions ownerPerm = e.getValue();

            boolean conflict =
                    // Write request conflicts with any other holder
                    (perm == Permissions.READ_WRITE)
                            // Read request conflicts only with existing writer
                            || (perm == Permissions.READ_ONLY && ownerPerm == Permissions.READ_WRITE);

            // Upgrade: if I already hold READ and now want WRITE, all *other* holders block
            // me
            if (!conflict && iHold && perm == Permissions.READ_WRITE) {
                conflict = true;
            }

            if (conflict)
                blockers.add(owner);
        }

        return blockers;
    }

    private boolean detectCycleFrom(TransactionId start) {
        Set<TransactionId> visited = new HashSet<>();
        Set<TransactionId> stack = new HashSet<>();
        return dfs(start, visited, stack);
    }

    private boolean dfs(TransactionId u, Set<TransactionId> visited, Set<TransactionId> stack) {
        if (!visited.add(u))
            return false;
        stack.add(u);
        for (TransactionId v : cycleGraph.getOrDefault(u, Collections.emptySet())) {
            if (!visited.contains(v) && dfs(v, visited, stack))
                return true;
            if (stack.contains(v))
                return true; // back-edge → cycle
        }
        stack.remove(u);
        return false;
    }

    private void removeAllEdgesOf(TransactionId tid) {
        cycleGraph.remove(tid);
        for (Set<TransactionId> tos : cycleGraph.values()) {
            tos.remove(tid);
        }
    }
}
