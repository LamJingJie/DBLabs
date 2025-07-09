package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private OpIterator child;
    private boolean hasDeleted;
    private TupleDesc tupleDesc;
    

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.transactionId = t;
        this.child = child;
        this.hasDeleted = false;
        this.tupleDesc = new TupleDesc( new Type[] { Type.INT_TYPE });
        // some code goes here
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

  public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        hasDeleted = false;
        // some code goes here
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        hasDeleted = false;
    }


    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (hasDeleted) return null;
        int count = 0;
        BufferPool bufferPool = Database.getBufferPool();

        while (child.hasNext()) {
            try {
                Tuple tuple = child.next();
                bufferPool.deleteTuple(transactionId, tuple);
                count++;
            } catch (Exception e) {
                throw new DbException("Insert failed: " + e.getMessage());
            }
        }
        Tuple result = new Tuple(getTupleDesc());
        result.setField(0, new IntField(count));
        hasDeleted = true;
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
