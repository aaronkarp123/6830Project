package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    private static int TIMEOUT_THRESHOLD = 2200;
    static boolean DEBUG_ON = false;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    private ConcurrentHashMap<PageId, Page> pageMap;
    private LockManager lm;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pageMap = new ConcurrentHashMap<PageId, Page>();
        lm = new LockManager();
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
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        // Acquire the proper lock first
        long start = System.currentTimeMillis();
        boolean success = lm.acquireLock(tid, pid, perm);
        while (!success) {
            try {
                long end = System.currentTimeMillis();
                if (end - start > TIMEOUT_THRESHOLD) {
                    System.out.println("Abort: waiting for Tid = " + tid.getId() + ", Pid = " + pid.toString() + ", Perm = " + perm.toString());
                    throw new TransactionAbortedException();
                }
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            success = lm.acquireLock(tid, pid, perm);
        }
        // Now we have the proper lock
        if (!pageMap.containsKey(pid)) {
           if (pageMap.size() == numPages)
               evictPage();
           pageMap.put(pid, Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
           pageMap.get(pid).setBeforeImage();
        }
        return pageMap.get(pid);
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lm.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lm.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        //System.out.println("TransactionComplete: Tid = " + tid.toString() + ", commit =" + commit);
        for (PageId pid : pageMap.keySet()) {
            if (pageMap.get(pid).isPageDirty() != null && pageMap.get(pid).isPageDirty().equals(tid)) {
                if (commit) {
                    flushPage(pid);
                } else {
                    pageMap.put(pid, pageMap.get(pid).getBeforeImage());
                }
            }
        }
        lm.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markPageDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        //System.out.println("Buffer pool try to insert tuple: Tid is" + tid.toString() + " Insert Tuple is" + ((IntField)(t.getField(0))).getValue());
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dpList = f.insertTuple(tid, t);
        // Now let's insert all dirty pages back to BufferPool
        for (Page p : dpList) {
            PageId pid = p.getId();
            if (!pageMap.containsKey(pid) && pageMap.size() == numPages) evictPage();
            pageMap.put(pid, p);
            pageMap.get(pid).markPageDirty(true, tid);
        }
        //System.out.println("Buffer pool succeeds to insert tuple: Tid is" + tid.toString() + " Insert Tuple is" + ((IntField)(t.getField(0))).getValue());
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markPageDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        //System.out.println("Buffer pool try to delete tuple: Tid is" + tid.toString() + " Delete Tuple is" + ((IntField)(t.getField(0))).getValue());
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> dpList = f.deleteTuple(tid, t);
        for (Page p : dpList) {
            PageId pid = p.getId();
            if (!pageMap.containsKey(pid) && pageMap.size() == numPages) evictPage();
            pageMap.put(pid, p);
            pageMap.get(pid).markPageDirty(true, tid);
        }
        //System.out.println("Buffer pool succeeds to delete tuple: Tid is" + tid.toString() + " Delete Tuple is" + ((IntField)(t.getField(0))).getValue());
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid : pageMap.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page p = pageMap.get(pid);
        if (p != null && p.isPageDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
            p.markPageDirty(false, null);
            p.setBeforeImage();
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : pageMap.keySet()) {
            Page p = pageMap.get(pid);
            if (p.isPageDirty() != null && p.isPageDirty().equals(tid)) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        ArrayList<PageId> cleanPages = new ArrayList<PageId>();
        for (PageId pid : pageMap.keySet()) {
            if (pageMap.get(pid).isPageDirty() == null) {
                cleanPages.add(pid);
            }
        }
        if (cleanPages.size() == 0) throw new DbException("No clean pages to evict!");
        PageId vic = cleanPages.get((int) Math.floor(Math.random() * cleanPages.size()));
        try {
            assert pageMap.get(vic).isPageDirty() == null : "Evict a dirty page!";
            flushPage(vic);
        } catch (Exception e) {
            e.printStackTrace();
        }
        pageMap.remove(vic);
    }

    private class LockManager {
        private HashMap<PageId, Set<TransactionId>> sharers;
        private HashMap<PageId, TransactionId> owners;
        private HashMap<TransactionId, Set<PageId>> sharedPages;
        private HashMap<TransactionId, Set<PageId>> ownedPages;

        private HashMap<PageId, Set<TransactionId>> waiters;
        private HashMap<TransactionId, Set<PageId>> waitedPages;

        public LockManager() {
            sharers = new HashMap<PageId, Set<TransactionId>>();
            owners = new HashMap<PageId, TransactionId>();
            sharedPages = new HashMap<TransactionId, Set<PageId>>();
            ownedPages = new HashMap<TransactionId, Set<PageId>>();
            waiters = new HashMap<PageId, Set<TransactionId>>();
            waitedPages = new HashMap<TransactionId, Set<PageId>>();
        }
        public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) {
            checkConsistency();
            //System.out.println("************************************************************************************************************************Want: Tid = " + tid.toString() + ", Pid = " + pid.toString() + ", Perm = " + perm.toString());
            boolean success = false;
            if (perm.equals(Permissions.READ_WRITE)) {
                success = acquireExclusiveLock(tid, pid);
            } else if (perm.equals(Permissions.READ_ONLY)) {
                success = acquireSharedLock(tid, pid);
            } else if (perm.equals(Permissions.NO_LOCK)) {
            	if (BufferPool.DEBUG_ON) System.out.println("Tx "+tid.getId() +" Nol "+pid);
                success = true;
            } else {
                assert false : "What?!";
            }
            if (success) {
                //System.out.println("Success: Tid = " + tid.toString() + ", Pid = " + pid.toString() + ", Perm = " + perm.toString());
                return true;
            } else {
                detectDeadlock(tid, pid);
                return false;
            }
        }
        private void detectDeadlock(TransactionId tid, PageId pid) {
            return;
        }
        private void checkConsistency() {
            for (PageId pid : sharers.keySet()) {
                assert !owners.keySet().contains(pid) : "Page in sharers is also in owners.";
                assert sharers.get(pid).size() > 0 : "sharer is actually empty.";
                for (TransactionId tid : sharers.get(pid)) {
                    assert sharedPages.get(tid).contains(pid) : "Pair in sharers does not exist in sharedPages.";
                }
            }
            for (PageId pid : owners.keySet()) {
                assert !sharers.keySet().contains(pid) : "Page in owners is also in sharers.";
                TransactionId tid = owners.get(pid);
                assert owners.get(pid) != null : "owner is actually empty.";
                assert ownedPages.get(tid).contains(pid) : "Pair in owners does not exist in ownedPages.";
            }
            for (TransactionId tid : sharedPages.keySet()) {
                assert sharedPages.get(tid).size() > 0 : "SharedPages is actually empty.";
                for (PageId pid : sharedPages.get(tid)) {
                    assert sharers.get(pid).contains(tid) : "Pair in sharedPages does not exist in sharers.";
                }
            }
            for (TransactionId tid : ownedPages.keySet()) {
                assert ownedPages.get(tid).size() > 0 : "OwnedPages is actually empty.";
                for (PageId pid : ownedPages.get(tid)) {
                    assert owners.get(pid).equals(tid) : "Pair in ownedPages does not exist in owners.";
                }
            }
        }
        public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
            checkConsistency();
            TransactionId owner = owners.get(pid);
            if (owner != null && owner.equals(tid)) return true;
            Set<TransactionId> sharer = sharers.get(pid);
            if (sharer != null && sharer.contains(tid)) return true;
            return false;
        }
        private void addSharer(TransactionId tid, PageId pid) {
            Set<TransactionId> sharer = sharers.get(pid);
            if (sharer == null) sharer = new HashSet<TransactionId>();
            sharer.add(tid);
            sharers.put(pid, sharer);
            Set<PageId> sharedPage = sharedPages.get(tid);
            if (sharedPage == null) sharedPage = new HashSet<PageId>();
            sharedPage.add(pid);
            sharedPages.put(tid, sharedPage);
        }
        private void addOwner(TransactionId tid, PageId pid) {
            owners.put(pid, tid);
            Set<PageId> ownedPage = ownedPages.get(tid);
            if (ownedPage == null) ownedPage = new HashSet<PageId>();
            ownedPage.add(pid);
            ownedPages.put(tid, ownedPage);
        }
        private void removeSharer(TransactionId tid, PageId pid) {
            Set<TransactionId> sharer = sharers.get(pid);
            sharer.remove(tid);
            if (sharer.size() == 0) sharers.remove(pid);
            else sharers.put(pid, sharer);
            Set<PageId> sharedPage = sharedPages.get(tid);
            sharedPage.remove(pid);
            if (sharedPage.size() == 0) sharedPages.remove(tid);
            else sharedPages.put(tid, sharedPage);
        }
        private void removeOwner(TransactionId tid, PageId pid) {
            owners.remove(pid);
            Set<PageId> ownedPage = ownedPages.get(tid);
            ownedPage.remove(pid);
            if (ownedPage.size() == 0) ownedPages.remove(tid);
            else ownedPages.put(tid, ownedPage);
        }
        private boolean acquireExclusiveLock(TransactionId tid, PageId pid) {
            checkConsistency();
            Set<TransactionId> sharer = sharers.get(pid);
            TransactionId owner = owners.get(pid);
            assert owner == null || sharer == null : "owner and sharer are not null at the same time!";
            // There is an existing owner other than itself
            if (owner != null && !owner.equals(tid)) return false;
            // There is an existing sharer other than itself
            if (sharer != null && ( (sharer.size() > 1) || (sharer.size() == 1 && !sharer.contains(tid)) )) return false;
            if (sharer != null) {
                // It holds the shared permission already
                assert (sharer.size() == 1 && sharer.contains(tid)) : "Consistency issue.";
                removeSharer(tid, pid);
            }
            addOwner(tid, pid);
            checkConsistency();
            if (BufferPool.DEBUG_ON) System.out.println("Tx "+ tid.getId() + " Xlock Acq "+ pid);
            return true;
        }
        private boolean acquireSharedLock(TransactionId tid, PageId pid) {
            checkConsistency();
            Set<TransactionId> sharer = sharers.get(pid);
            TransactionId owner = owners.get(pid);
            System.out.println("Tx "+ tid.getId() + " Slock Acq "+ pid);
            assert owner == null || sharer == null : "owner and sharer are not null at the same time!";
            if (owner != null && !owner.equals(tid)) {
                // There is an existing owner other than itself
                return false;
            } else {
                // (owner != null) means it holds the exclusive permission, thus does nothing
                if (owner == null) addSharer(tid, pid);
                checkConsistency();
                return true;
            }
        }
        public synchronized void releaseLock(TransactionId tid, PageId pid) {
            checkConsistency();
            //System.out.println("ReleaseLocks: Tid = " + tid.toString() + ", Pid = " + pid.toString());
            Set<PageId> sharedPage = sharedPages.get(tid);
            if (sharedPage != null && sharedPage.contains(pid)) {
                removeSharer(tid, pid);
            }
            Set<PageId> ownedPage = ownedPages.get(tid);
            if (ownedPage != null && ownedPage.contains(pid)) {
                removeOwner(tid, pid);
            }
            if (BufferPool.DEBUG_ON) System.out.println("Tx "+ tid.getId() + " Xlock Rel "+ pid);
        }
        public synchronized void releaseAllLocks(TransactionId tid) {
            checkConsistency();
            //System.out.println("ReleaseAllLocks: Tid = " + tid.toString());
            if (sharedPages.get(tid) != null) {
                for (PageId pid : sharedPages.get(tid)) {
                    Set<TransactionId> sharer = sharers.get(pid);
                    sharer.remove(tid);
                    if (sharer.size() == 0) sharers.remove(pid);
                    else sharers.put(pid, sharer);
                }
                sharedPages.remove(tid);
            }
            if (ownedPages.get(tid) != null) {
                for (PageId pid : ownedPages.get(tid)) {
                    owners.remove(pid);
                }
                ownedPages.remove(tid);
            }
        }
    }
}
