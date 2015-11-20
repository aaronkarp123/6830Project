package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int count;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        count = 0;
        called = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] tps = new Type[1];
        tps[0] = Type.INT_TYPE;
        return new TupleDesc(tps);
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        if (child == null) throw new DbException("Child is null");
        child.open();
        super.open();
        count = 0;
        called = false;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        count = 0;
        called = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        count = 0;
        called = false;
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
        // some code goes here
        if (child == null || called) return null;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(tid, child.next());
            } catch (IOException e) {
                e.printStackTrace();
            }
            count ++;
        }
        Tuple ans = new Tuple(getTupleDesc());
        ans.setField(0, new IntField(count));
        called = true;
        return ans;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[1];
        children[0] = child;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child = children[0];
    }

}
