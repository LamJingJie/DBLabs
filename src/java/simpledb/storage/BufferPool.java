package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    private final HashMap<PageId, Page> bufferpoolcache;

    // Clock replacement policy fields
    private final HashMap<PageId, Integer> referenceBits;
    private final ArrayList<PageId> circularList; // Pages in order of access for clock pointer using page id
    private int clockPointer;

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    public final int numPages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.bufferpoolcache = new HashMap<>();

        // Initialize fields for clock replacement policy
        this.referenceBits = new HashMap<PageId, Integer>();
        this.circularList = new ArrayList<PageId>();
        this.clockPointer = 0;
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    private final LockManager lockManager = new LockManager();

    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        lockManager.acquireLock(pid, tid, perm);

        // Found in pool
        if (this.bufferpoolcache.containsKey(pid)) {
            referenceBits.put(pid, 1); // Put reference bit when accessing page
            return this.bufferpoolcache.get(pid);
        }

        int tableid = pid.getTableId();

        Catalog databasecatalog = Database.getCatalog();

        DbFile databasefile = databasecatalog.getDatabaseFile(tableid);

        Page page = databasefile.readPage(pid);

        if (bufferpoolcache.size() >= numPages) {
            evictPage(); // Evict a page if buffer pool is full before next is loaded
        }

        // When page not in buffer pool, load new page in
        bufferpoolcache.put(pid, page);
        referenceBits.put(pid, 1);
        circularList.add(pid);

        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbFile.insertTuple(tid, t);
        markpages(pages, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbFile.deleteTuple(tid, t);
        markpages(pages, tid);
    }

    // added this function to mark pages as dirty and update the buffer pool cache
    private void markpages(List<Page> pages, TransactionId tid) {
        for (Page page : pages) {
            page.markDirty(true, tid);
            bufferpoolcache.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

        for (Page page : bufferpoolcache.values()) {
            if (page.isDirty() != null) {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1

        // Clear the page from buffer pool and its fields, no need to flush
        bufferpoolcache.remove(pid);
        referenceBits.remove(pid);
        circularList.remove(pid);

        if (clockPointer >= circularList.size() && !circularList.isEmpty()) {
            clockPointer = 0;
        }
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        if (bufferpoolcache.containsKey(pid)) {
            Page page = bufferpoolcache.get(pid);
            // If page is dirty, write it to disk to update the disk
            if (page.isDirty() != null) {
                int tableId = pid.getTableId();
                DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
                dbFile.writePage(page);

                page.markDirty(false, null);
            }
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        // Using clock replacement policy to evict out a page
        if (circularList.isEmpty()) {
            throw new DbException("No pages in buffer pool cache to evict");
        }

        PageId evictedPid = null;

        // Check attempts to prevent infinite loop since the reference bits would get
        // reset due to second chance
        int attempts = 0;
        int maxAttempts = circularList.size() * 2;

        // Find a page with reference bit = 0, if not found, use second chance
        while (attempts < maxAttempts) {
            if (clockPointer >= circularList.size()) {
                clockPointer = 0;
            }

            PageId currentPid = circularList.get(clockPointer); // pid at current clock pointer

            // If current page not in buffer pool but still in list on accident
            if (!bufferpoolcache.containsKey(currentPid)) {
                circularList.remove(clockPointer);
                referenceBits.remove(currentPid);
                continue;
            }

            Integer currentRefBit = referenceBits.get(currentPid);
            if (currentRefBit == null) {
                throw new DbException("referenceBits is null for page: " + currentPid);
            }

            // No Steal: Cannot evict a dirty page
            // Or if page is holding lock,
            // Skip over to next possible evictable page
            if (currentRefBit == 0) {
                Page candidatePage = bufferpoolcache.get(currentPid);
                // Not dirty and not holding lock, can evict
                if (candidatePage.isDirty() == null || !lockManager.isHoldingLock(currentPid)) {
                    evictedPid = currentPid;
                    bufferpoolcache.remove(evictedPid);
                    referenceBits.remove(evictedPid);
                    circularList.remove(evictedPid);
                    break;
                }
                // Skip if dirty or has lock
                continue;
            }
            // Reference bit is 1, change to 0 giving it a second chance, move pointer to
            // next
            else {
                referenceBits.put(currentPid, 0);
            }
            clockPointer++;
            attempts++;
        }

        // If no evictable page found after max attempts, throw exception
       if (evictedPid == null) {
            throw new DbException("All pages are dirty, cannot evict any page");
        }

    }

}
