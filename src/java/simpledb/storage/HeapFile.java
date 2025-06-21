package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
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
     *            the file that stores the on-disk backing store for this heap
     *            file.
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
    public Page readPage(PageId pid) throws IllegalArgumentException{
        // some code goes here
    /**
     * Read the specified page from disk.
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
        // Check if position goes out of bounds of the file's length, catch exception earlier, before reading which causes the IOException
        if (position >= raf.length()) {
            throw new IllegalArgumentException("Page with id " + pid + "  does not exist in this file");
        }
        // Find and Read the page array from the file, jump to the position of the page
        raf.seek(position);
        // We use readFully to ensure we read the entire page data, not incomplete data. Safer than read()
        raf.readFully(pageData);
        // Create a HeapPage object with the read data, for returning
        page = new HeapPage((HeapPageId) pid, pageData);
    }
    catch (IOException e) {
        throw new IllegalArgumentException("IOException: Page with id " + pid + " does not exist in this file");
    }
    return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // Number of pages is the length of this heapfile + pagesize-1, divided by the page size
        // To ensure that we round up to the next page if there is any remaining data
        int pages = (int) ((f.length() + BufferPool.getPageSize() - 1 )/ BufferPool.getPageSize());
        return pages;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
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
        //We want an iterator that iterates through the pages of the heapfile, going through each tuple in each page
        // Methods: open(), close(), hasNext, next(), rewind()
        return new DbFileIterator(){
            private final int numPages = numPages();
            private int currentPageIndex = 0;
            // Iterator iterates the tuples in the current page
            private Iterator<Tuple> tupleIterator = null;
            private boolean opened = false; 

            // Open method is to initialize iterator to initial state
            public void open() throws DbException, TransactionAbortedException {
                // Initialize the current page index to 0 and tuple iterator to null
                currentPageIndex = 0;
                tupleIterator = null;
                opened = true;
            }

            // Close method is to reset the iterator to closed state
            public void close() {
                if (opened){ 
                    currentPageIndex = numPages; // Indicate that no more pages left for hasNext();
                    // Reset the tuple iterator to null, clean up memory for tables, wont allow access to methods: hasNext(), next(), rewind()
                    tupleIterator = null;
                    opened = false;
                }
            }

            public boolean hasNext() throws DbException, TransactionAbortedException {
                // System.out.println("opened=" + opened);

                if (!opened) {
                    return false; // If iterator is not opened, return false
                }

                // Check if tuple iterator exists and if it has a next tuple
                if (tupleIterator != null && tupleIterator.hasNext()) {
                    return true;
                }
                // Current page has no more tuples, so we move to next page for more tuples till last page
                while (currentPageIndex < numPages) {
                    // Get the next page from the buffer pool
                    HeapPageId pid = new HeapPageId(getId(), currentPageIndex);
                    Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                    // Create a new iterator for the tuples in this page
                    tupleIterator = ((HeapPage) page).iterator();
                    currentPageIndex++;

                
                    if (tupleIterator.hasNext()) {
                        return true;
                    }
                }
                // No more tuples, end of all pages
                return false;
            }

            // Returns the next tuple in the iterator
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!opened || tupleIterator == null || !tupleIterator.hasNext()) {
                    throw new NoSuchElementException();
                }
                // Return the next tuple from the current page's iterator
                return tupleIterator.next();
            }

            // Rewind method is to reset the iterator to the initial state using open()
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

        };
    }

}

