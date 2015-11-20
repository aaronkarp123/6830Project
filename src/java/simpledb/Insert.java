package simpledb;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int tableid;

    private int count;
    private boolean called;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        if (child == null || !child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableid)))
            throw new DbException("TupleDesc does not match.");
        this.child = child;
        this.tid = t;
        this.tableid = tableid;
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (child == null || called) return null;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(tid, tableid, child.next());
            } catch (Exception e) {
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
