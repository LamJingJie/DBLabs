package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

import javax.xml.crypto.Data;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *          the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        /**
         * Read the specified page from disk.
         * 
         * @throws IllegalArgumentException if the page does not exist in this file.
         */

        Page page = null;
        int pageSize = BufferPool.getPageSize();
        // Get page number to read
        int pageNumber = pid.getPageNumber();
        // Create a byte[] array to hold the page data
        byte[] pageData = new byte[pageSize];
        // Try to access and read the page from the file
        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            // Find the page's position/offset inside the heapfile
            int position = pageNumber * pageSize;
            // Check if position goes out of bounds of the file's length, catch exception
            // earlier, before reading which causes the IOException
            if (position >= raf.length()) {
                throw new IllegalArgumentException("Page with id " + pid + "  does not exist in this file");
            }
            // Find and Read the page array from the file, jump to the position of the page
            raf.seek(position);
            // We use readFully to ensure we read the entire page data, not incomplete data.
            // Safer than read()
            raf.readFully(pageData);
            // Create a HeapPage object with the read data, for returning
            page = new HeapPage((HeapPageId) pid, pageData);
        } catch (IOException e) {
            throw new IllegalArgumentException("IOException: Page with id " + pid + " does not exist in this file");
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pageSize = BufferPool.getPageSize();
        int pageNumber = page.getId().getPageNumber();
        byte[] data = page.getPageData();

        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            int position = pageNumber * pageSize;
            raf.seek(position);
            raf.write(data);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // Number of pages is the length of this heapfile + pagesize-1, divided by the
        // page size
        // To ensure that we round up to the next page if there is any remaining data
        int pages = (int) ((f.length() + BufferPool.getPageSize() - 1) / BufferPool.getPageSize());
        return pages;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> modifiedPages = new ArrayList<>();
        HeapPage pageToInsert = null;

        // 1. Find a page with an empty slot. Use read locks to avoid unnecessary
        // write-locking.
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (p.getNumEmptySlots() > 0) {
                // Found a candidate page. We can now get a write lock on it.
                // The read lock will be upgraded to a write lock by the LockManager.
                pageToInsert = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                break;
            }
        }

        // 2. If no existing page has space, create a new one.
        if (pageToInsert == null) {
            // Create a new empty page and write it to the file.
            HeapPageId newPid = new HeapPageId(getId(), numPages());
            HeapPage newPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
            writePage(newPage); // Appends the page to the file

            // Get the new page from the buffer pool with a write lock to insert into it.
            pageToInsert = (HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
        }

        // 3. Insert the tuple into the chosen page (either an existing one or a new
        // one).
        pageToInsert.insertTuple(t);
        modifiedPages.add(pageToInsert);

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        RecordId rid = t.getRecordId();
        if (rid == null) {
            throw new DbException("Tuple does not have a valid RecordId");
        }

        PageId pid = rid.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);

        ArrayList<Page> modifiedPages = new ArrayList<>();
        modifiedPages.add(page);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        /**
         * Returns an iterator over all the tuples stored in this DbFile. The
         * iterator must use {@link BufferPool#getPage}, rather than
         * {@link #readPage} to iterate through the pages.
         *
         * @return an iterator over all the tuples stored in this DbFile.
         */
        // We want an iterator that iterates through the pages of the heapfile, going
        // through each tuple in each page
        // Methods: open(), close(), hasNext, next(), rewind()
        return new DbFileIterator() {
            private final int numPages = numPages();
            private int currentPageIndex = 0;
            // Iterator iterates the tuples in the current page
            private Iterator<Tuple> tupleIterator = null;
            private Tuple nextTuple = null;
            private boolean opened = false;

            // Prefetches the next tuple, if tuple found, stored in nextTuple
            private void prefetch() throws DbException, TransactionAbortedException {
                if (nextTuple != null) {
                    return; // alrdy have 1
                }

                while (currentPageIndex < numPages) {

                    if (tupleIterator == null) {
                        HeapPageId pid = new HeapPageId(getId(), currentPageIndex);
                        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                        tupleIterator = page.iterator();
                    }

                    if (tupleIterator.hasNext()) {
                        nextTuple = tupleIterator.next();
                        return;
                    } else {
                        // Move to next page
                        currentPageIndex++;
                        tupleIterator = null;
                    }
                }
            }

            // Open method is to initialize iterator to initial state
            public void open() throws DbException, TransactionAbortedException {
                // Initialize the current page index to 0 and tuple iterator to null
                currentPageIndex = 0;
                tupleIterator = null;
                opened = true;
                nextTuple = null;
            }

            // Close method is to reset the iterator to closed state
            public void close() {

                // Reset the tuple iterator to null, clean up memory for tables, wont allow
                // access to methods: hasNext(), next(), rewind()
                tupleIterator = null;
                opened = false;
                nextTuple = null;

            }

            public boolean hasNext() throws DbException, TransactionAbortedException {
                // System.out.println("opened=" + opened);
                if (!opened) {
                    return false; // If iterator is not opened, return false
                }

                prefetch();
                return nextTuple != null;
            }

            // Returns the next tuple in the iterator
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {

                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                // Return the next tuple from the current page's iterator
                Tuple result = nextTuple;
                nextTuple = null;
                return result;
            }

            // Rewind method is to reset the iterator to the initial state using open()
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

        };
    }

}
