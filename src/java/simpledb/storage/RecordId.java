package simpledb.storage;
import java.util.Objects;
import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PageId pid;
    private final int tupleno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pid=  pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return this.tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return this.pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        //Check if the object is even an HeapPageId object
        if (!(o instanceof RecordId)){
            return false;
        }
        // Cast ( since it was a generic object to HeapPageId and since we know that it is an
        //instance of HeapPage )
        RecordId castrecordId = (RecordId) o;
        //The reason why both pid and tupleno is use is because pid is the page number and tupleno
        //points to entry in the page.
        return (castrecordId.pid.equals(pid) && castrecordId.tupleno == this.tupleno); 
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return Objects.hash(pid,tupleno);

    }

}
