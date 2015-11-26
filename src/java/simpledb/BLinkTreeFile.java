package simpledb;

import java.io.*;
import java.util.*;
import java.nio.channels.FileChannel;

import simpledb.Predicate.Op;

/**
 * BTreeFile is an implementation of a DbFile that stores a B+ tree.
 * Specifically, it stores a pointer to a root page,
 * a set of internal pages, and a set of leaf pages, which contain a collection of tuples
 * in sorted order. BTreeFile works closely with BTreeLeafPage, BTreeInternalPage,
 * and BTreeRootPtrPage. The format of these pages is described in their constructors.
 * 
 * @see simpledb.BTreeLeafPage#BTreeLeafPage
 * @see simpledb.BTreeInternalPage#BTreeInternalPage
 * @see simpledb.BTreeHeaderPage#BTreeHeaderPage
 * @see simpledb.BTreeRootPtrPage#BTreeRootPtrPage
 * @author Becca Taft
 */
public class BLinkTreeFile extends BTreeFile {


	/**
	 * Constructs a B+ tree file backed by the specified file.
	 * 
	 * @param f - the file that stores the on-disk backing store for this B+ tree
	 *            file.
	 * @param key - the field which index is keyed on
	 * @param td - the tuple descriptor of tuples in the file
	 */
	public BLinkTreeFile(File f, int key, TupleDesc td) {
		super(f,key,td);
	}

	/**
	 * Returns the File backing this BTreeFile on disk.
	 */


	/**
	 * Read a page from the file on disk. This should not be called directly
	 * but should be called from the BufferPool via getPage()
	 * 
	 * @param pid - the id of the page to read from disk
	 * @return the page constructed from the contents on disk
	 */
	public Page readPage(PageId pid) {
		BTreePageId id = (BTreePageId) pid;
		BufferedInputStream bis = null;

		try {
			bis = new BufferedInputStream(new FileInputStream(f));
			if(id.pgcateg() == BTreePageId.ROOT_PTR) {
				byte pageBuf[] = new byte[BTreeRootPtrPage.getPageSize()];
				int retval = bis.read(pageBuf, 0, BTreeRootPtrPage.getPageSize());
				if (retval == -1) {
					throw new IllegalArgumentException("Read past end of table");
				}
				if (retval < BTreeRootPtrPage.getPageSize()) {
					throw new IllegalArgumentException("Unable to read "
							+ BTreeRootPtrPage.getPageSize() + " bytes from BTreeFile");
				}
				Debug.log(1, "BTreeFile.readPage: read page %d", id.pageNumber());
				BTreeRootPtrPage p = new BTreeRootPtrPage(id, pageBuf);
				return p;
			}
			else {
				byte pageBuf[] = new byte[BufferPool.getPageSize()];
				if (bis.skip(BTreeRootPtrPage.getPageSize() + (id.pageNumber()-1) * BufferPool.getPageSize()) != 
						BTreeRootPtrPage.getPageSize() + (id.pageNumber()-1) * BufferPool.getPageSize()) {
					throw new IllegalArgumentException(
							"Unable to seek to correct place in BTreeFile");
				}
				int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
				if (retval == -1) {
					throw new IllegalArgumentException("Read past end of table");
				}
				if (retval < BufferPool.getPageSize()) {
					throw new IllegalArgumentException("Unable to read "
							+ BufferPool.getPageSize() + " bytes from BTreeFile");
				}
				Debug.log(1, "BTreeFile.readPage: read page %d", id.pageNumber());
				if(id.pgcateg() == BTreePageId.INTERNAL) {
					BLinkTreeInternalPage p = new BLinkTreeInternalPage(id, pageBuf, keyField);
					return p;
				}
				else if(id.pgcateg() == BTreePageId.LEAF) {
					BLinkTreeLeafPage p = new BLinkTreeLeafPage(id, pageBuf, keyField);
					return p;
				}
				else { // id.pgcateg() == BTreePageId.HEADER
					BTreeHeaderPage p = new BTreeHeaderPage(id, pageBuf);
					return p;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			// Close the file on success or error
			try {
				if (bis != null)
					bis.close();
			} catch (IOException ioe) {
				// Ignore failures closing the file
			}
		}
	}



	/**
	 * Recursive function which finds and locks the leaf page in the B+ tree corresponding to
	 * the left-most page possibly containing the key field f. It locks all internal
	 * nodes along the path to the leaf node with READ_ONLY permission, and locks the 
	 * leaf node with permission perm.
	 * 
	 * If f is null, it finds the left-most leaf page -- used for the iterator
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - the current page being searched
	 * @param perm - the permissions with which to lock the leaf page
	 * @param f - the field to search for
	 * @return the left-most leaf page possibly containing the key field f
	 * 
	 */
	private BLinkTreeLeafPage findLeafPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePageId pid, Permissions perm,
			Field f, Stack<PageId> stack) 
					throws DbException, TransactionAbortedException {
		// some code goes here
            switch (pid.pgcateg()) {
                case BTreePageId.LEAF:
                	BLinkTreeLeafPage cur = (BLinkTreeLeafPage) this.getPage(tid, dirtypages, pid, perm);
                	BLinkTreeLeafPage new_cur;
                	while (cur.getRightSiblingId() != null && !f.compare(Op.LESS_THAN, cur.getHighKey())) {             		
                		new_cur = (BLinkTreeLeafPage) this.getPage(tid, dirtypages, cur.getRightSiblingId(), perm);
                		Database.getBufferPool().releasePage(tid, cur.getId());
                		cur = new_cur;
                	}
                    return cur;
                case BTreePageId.INTERNAL:
                    BLinkTreeInternalPage pg = (BLinkTreeInternalPage)(this.getPage(tid, dirtypages, pid, Permissions.NO_LOCK));
                    Iterator<BTreeEntry> es = pg.iterator();
                    if (es == null || !es.hasNext())
                        throw new DbException("Illegal entry iterator.");
                    if (f == null) {
                    	stack.push(pg.getId());
                        return findLeafPage(tid, dirtypages, es.next().getLeftChild(), perm, f, stack);
                    }
                    while (pg.getRightSiblingId() != null && !f.compare(Op.LESS_THAN, pg.getHighKey())){
                    	pg = (BLinkTreeInternalPage)(this.getPage(tid, dirtypages, pg.getRightSiblingId(), Permissions.NO_LOCK));
                    }
                    stack.push(pg.getId());
                    BTreeEntry e = es.next();
                    while (true) {
                        if (e.getKey().compare(Op.GREATER_THAN_OR_EQ, f)) {
                        	
                        	return findLeafPage(tid, dirtypages, e.getLeftChild(), perm, f,stack);
                        }
                            
                        if (es.hasNext()) e = es.next();
                        else break;
                    }
                    return findLeafPage(tid, dirtypages, e.getRightChild(), perm, f,stack);
                case BTreePageId.HEADER:
                case BTreePageId.ROOT_PTR:
                default:
                    throw new DbException("Illegal pageid type.");
            }
	}
	
	/**
	 * Convenience method to find a leaf page when there is no dirtypages HashMap.
	 * Used by the BTreeFile iterator.
	 * @see #findLeafPage(TransactionId, HashMap, BTreePageId, Permissions, Field)
	 * 
	 * @param tid - the transaction id
	 * @param pid - the current page being searched
	 * @param perm - the permissions with which to lock the leaf page
	 * @param f - the field to search for
	 * @return the left-most leaf page possibly containing the key field f
	 * 
	 */
	BTreeLeafPage findLeafPage(TransactionId tid, BTreePageId pid, Permissions perm,
			Field f) 
					throws DbException, TransactionAbortedException {
		return findLeafPage(tid, new HashMap<PageId, Page>(), pid, perm, f, new Stack<PageId>());
	}

	/**
	 * Split a leaf page to make room for new tuples and recursively split the parent node
	 * as needed to accommodate a new entry. The new entry should have a key matching the key field
	 * of the first tuple in the right-hand page (the key is "copied up"), and child pointers 
	 * pointing to the two leaf pages resulting from the split.  Update sibling pointers and parent 
	 * pointers as needed.  
	 * 
	 * Return the leaf page into which a new tuple with key field "field" should be inserted.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the leaf page to split
	 * @param field - the key field of the tuple to be inserted after the split is complete. Necessary to know
	 * which of the two pages to return.
	 * @see #getParentWithEmptySlots(TransactionId, HashMap, BTreePageId, Field)
	 * 
	 * @return the newly created page
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	protected BLinkTreeLeafPage splitLeafPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BLinkTreeLeafPage page, Field field) 
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
        //
        // Split the leaf page by adding a new page on the right of the existing
		// page and moving half of the tuples to the new page.  Copy the middle key up
		// into the parent page, and recursively split the parent as needed to accommodate
		// the new entry.  getParentWithEmtpySlots() will be useful here.  Don't forget to update
		// the sibling pointers of all the affected leaf pages.  Return the page into which a 
		// tuple with the given key field should be inserted.
		 BLinkTreeLeafPage npage = (BLinkTreeLeafPage)getEmptyPage(tid, dirtypages, BTreePageId.LEAF);
		 Iterator<Tuple> ts = page.iterator();
		 if (ts == null || !ts.hasNext()) throw new DbException("Illegal tuple iterator.");
		 // Move (left half) tuples to the new leaf page
		 int numTs = page.getNumTuples();
		 ArrayList<Tuple> arrt = new ArrayList<Tuple>();
		
		 for (int j = 0; j < numTs / 2; j ++) {
			 ts.next();
		 }
		 while(ts.hasNext()) {
			 arrt.add(ts.next());
		 }
		 for (Tuple t: arrt) {
		     page.deleteTuple(t);
		     npage.insertTuple(t);
		 }
		 // Set sibling poiters
	     npage.setRightSiblingId(page.getRightSiblingId());      
		 page.setRightSiblingId(npage.getId());
		 // Set the entry (and field) that should be inserted to their parent
		 Field f = arrt.get(0).getField(keyField);
		 npage.setHighKey(page.getHighKey());
		 page.setHighKey(f);
         
         // Link the (probably new) parent and the leaf nodes
         /*parent.insertEntry(e);
         updateParentPointers(tid, dirtypages, parent);*/
         // Compare the input field with the new field in parent
         return npage;
         //return (f.compare(Op.GREATER_THAN_OR_EQ, field)) ? npage : page;
	}
	
	
	/**
	 * Split an internal page to make room for new entries and recursively split its parent page
	 * as needed to accommodate a new entry. The new entry for the parent should have a key matching 
	 * the middle key in the original internal page being split (this key is "pushed up" to the parent). 
	 * The child pointers of the new parent entry should point to the two internal pages resulting 
	 * from the split. Update parent pointers as needed.
	 * 
	 * Return the internal page into which an entry with key field "field" should be inserted
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page to split
	 * @param field - the key field of the entry to be inserted after the split is complete. Necessary to know
	 * which of the two pages to return.
	 * @see #getParentWithEmptySlots(TransactionId, HashMap, BTreePageId, Field)
	 * @see #updateParentPointers(TransactionId, HashMap, BTreeInternalPage)
	 * 
	 * @return the internal page into which the new entry should be inserted
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	protected BLinkTreeInternalPage splitInternalPage(TransactionId tid, HashMap<PageId, Page> dirtypages, 
			BLinkTreeInternalPage page, Field field) 
					throws DbException, IOException, TransactionAbortedException {
		// some code goes here
        //
        // Split the internal page by adding a new page on the right of the existing
		// page and moving half of the entries to the new page.  Push the middle key up
		// into the parent page, and recursively split the parent as needed to accommodate
		// the new entry.  getParentWithEmtpySlots() will be useful here.  Don't forget to update
		// the parent pointers of all the children moving to the new page.  updateParentPointers()
		// will be useful here.  Return the page into which an entry with the given key field
		// should be inserted.
		BLinkTreeInternalPage npage = (BLinkTreeInternalPage)getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);
        Iterator<BTreeEntry> es = page.iterator();
        if (es == null || !es.hasNext()) throw new DbException("Illegal entry iterator.");
        // Move (left half) entries to the new page
        int numEs = page.getNumEntries();
        ArrayList<BTreeEntry> arrt = new ArrayList<BTreeEntry>();
		
		for (int j = 0; j < numEs / 2; j ++) {
			es.next();
		}
		BTreeEntry mid = es.next();
		while(es.hasNext()) {
			 arrt.add(es.next());
		}
		// Pick the one in the middle
		page.deleteKeyAndRightChild(mid);
		for (BTreeEntry e: arrt) {
			page.deleteKeyAndRightChild(e);
			npage.insertEntry(e);
		}
        
        
        Field f_push = mid.getKey();
        mid = new BTreeEntry(f_push, page.getId(), npage.getId());
        
        npage.setRightSiblingId(page.getRightSiblingId());      
		page.setRightSiblingId(npage.getId());
		
		npage.setHighKey(page.getHighKey());
		page.setHighKey(f_push);
        
        // Compare the input field with the new field in parent
        return npage;
	}
	
	
	/**
	 * Method to encapsulate the process of locking/fetching a page.  First the method checks the local 
	 * cache ("dirtypages"), and if it can't find the requested page there, it fetches it from the buffer pool.  
	 * It also adds pages to the dirtypages cache if they are fetched with read-write permission, since 
	 * presumably they will soon be dirtied by this transaction.
	 * 
	 * This method is needed to ensure that page updates are not lost if the same pages are
	 * accessed multiple times.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - the id of the requested page
	 * @param perm - the requested permissions on the page
	 * @return the requested page
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	Page getPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePageId pid, Permissions perm)
			throws DbException, TransactionAbortedException {
		if(dirtypages.containsKey(pid)) {
			return dirtypages.get(pid);
		}
		else {
			Page p = Database.getBufferPool().getPage(tid, pid, perm);
			if(perm == Permissions.READ_WRITE) {
				dirtypages.put(pid, p);
			}
			return p;
		}
	}
	
	// returns the entry that should go in the parent (iff the pid had to be split)
	public BTreeEntry insert(TransactionId tid, Tuple t, BTreePageId pid, HashMap<PageId, Page> dirtypages ) throws DbException, IOException, TransactionAbortedException{
		
		switch (pid.pgcateg() ){
		case BTreePageId.ROOT_PTR:
			System.out.println("Should not happen");
			return null;
		case BTreePageId.INTERNAL:
			BLinkTreeInternalPage pg = (BLinkTreeInternalPage) this.getPage(tid, dirtypages, pid, Permissions.NO_LOCK);
			while (pg.getRightSiblingId() != null && !t.getField(keyField).compare(Op.LESS_THAN, pg.getHighKey())){
				pg = (BLinkTreeInternalPage) this.getPage(tid, dirtypages, pg.getRightSiblingId(), Permissions.NO_LOCK);
			}
			
			Iterator<BTreeEntry> es = pg.iterator();
            if (es == null || !es.hasNext())
                throw new DbException("Illegal entry iterator.");           
            Field f = t.getField(keyField);
            BTreePageId child;
            while (true) {
            	BTreeEntry e = es.next();
                if (e.getKey().compare(Op.GREATER_THAN_OR_EQ, f)) {
                	child = e.getLeftChild();
                	break;
                }                   
                if (!es.hasNext()) {
                	child = e.getRightChild();
                	break;
                }
            }
			
			BTreeEntry ret = insert(tid,t,child,dirtypages);
			// 
			pg = (BLinkTreeInternalPage) this.getPage(tid, dirtypages, pid, Permissions.READ_WRITE); // pop(stack)
			while (pg.getRightSiblingId() != null && !t.getField(keyField).compare(Op.LESS_THAN, pg.getHighKey())){
				BLinkTreeInternalPage old = pg;
				pg = (BLinkTreeInternalPage) this.getPage(tid, dirtypages, pg.getRightSiblingId(), Permissions.READ_WRITE);
				Database.getBufferPool().releasePage(tid, old.getId());
			}
			Database.getBufferPool().releasePage(tid, child);
			
			if (ret !=null) {
				if (pg.getNumEmptySlots() >0){
					pg.insertEntry(ret);
				} else {
					BLinkTreeInternalPage npage = splitInternalPage(tid, dirtypages, pg, t.getField(keyField));
					Field mid = pg.getHighKey();
					if (ret.getKey().compare(Op.GREATER_THAN_OR_EQ, mid)){
						npage.insertEntry(ret);
					} else {
						pg.insertEntry(ret);
					}
					BTreeEntry e = new BTreeEntry(mid, pg.getId(), npage.getId());
					//System.out.println("Has to split internal");
			        return e;
					
				}
			}
			return null;
		case BTreePageId.LEAF:
			BLinkTreeLeafPage page = (BLinkTreeLeafPage) this.getPage(tid, dirtypages, pid, Permissions.READ_WRITE);
			while (page.getRightSiblingId() != null && !t.getField(keyField).compare(Op.LESS_THAN, page.getHighKey())){
				BLinkTreeLeafPage old = page;
				page = (BLinkTreeLeafPage) this.getPage(tid, dirtypages, page.getRightSiblingId(), Permissions.READ_WRITE);
				Database.getBufferPool().releasePage(tid, old.getId());
			}
			if(page.getNumEmptySlots() > 0) {
				page.insertTuple(t);
				Database.getBufferPool().releasePage(tid, page.getId());
				return null;
			} else {
				BLinkTreeLeafPage npage = splitLeafPage(tid, dirtypages, page, t.getField(keyField));	
				Field mid = page.getHighKey();
				if (t.getField(keyField).compare(Op.GREATER_THAN_OR_EQ, mid)){
					npage.insertTuple(t);
				} else {
					page.insertTuple(t);
				}
				BTreeEntry e = new BTreeEntry(mid, page.getId(), npage.getId());
		         // Handle the parent
		        return e;
			}
			
		}
			
		
		return null;
	}

	/**
	 * Insert a tuple into this BTreeFile, keeping the tuples in sorted order. 
	 * May cause pages to split if the page where tuple t belongs is full.
	 * 
	 * @param tid - the transaction id
	 * @param t - the tuple to insert
	 * @return a list of all pages that were dirtied by this operation. Could include
	 * many pages since parent pointers will need to be updated when an internal node splits.
	 * @see #splitLeafPage(TransactionId, HashMap, BTreeLeafPage, Field)
	 */
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		HashMap<PageId, Page> dirtypages = new HashMap<PageId, Page>();
		
		BTreeRootPtrPage rootPtr;
		BTreePageId rootId;
		
		synchronized (this) {
			rootPtr = getRootPtrPage(tid, dirtypages);
			rootId = rootPtr.getRootId();
	
			if(rootId == null) { // the root has just been created, so set the root pointer to point to it		
				rootId = new BTreePageId(tableid, numPages(), BTreePageId.LEAF);
				rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
				rootPtr.setRootId(rootId);
			} 
		}
		
		BTreeEntry be = insert(tid,t,rootId,dirtypages);
		if (be != null){
			BLinkTreeInternalPage parent = (BLinkTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);

			// update the root pointer
			rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages,
					BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
			BTreePageId prevRootId = rootPtr.getRootId(); //save prev id before overwriting.
			rootPtr.setRootId(parent.getId());

			// update the previous root to now point to this new root.
			BTreePage prevRootPage = (BTreePage)getPage(tid, dirtypages, prevRootId, Permissions.READ_WRITE);
			prevRootPage.setParentId(parent.getId());
			parent.insertEntry(be);
		}
		
		ArrayList<Page> dirtyPagesArr = new ArrayList<Page>();
		dirtyPagesArr.addAll(dirtypages.values());
		return dirtyPagesArr;
	}
	
	/**
	 * Handle the case when a B+ tree page becomes less than half full due to deletions.
	 * If one of its siblings has extra tuples/entries, redistribute those tuples/entries.
	 * Otherwise merge with one of the siblings. Update pointers as needed.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the page which is less than half full
	 * @see #handleMinOccupancyLeafPage(TransactionId, HashMap, BTreeLeafPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
	 * @see #handleMinOccupancyInternalPage(TransactionId, HashMap, BTreeInternalPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */

	/**
	 * Delete a tuple from this BTreeFile. 
	 * May cause pages to merge or redistribute entries/tuples if the pages 
	 * become less than half full.
	 * 
	 * @param tid - the transaction id
	 * @param t - the tuple to delete
	 * @return a list of all pages that were dirtied by this operation. Could include
	 * many pages since parent pointers will need to be updated when an internal node merges.
	 * @see #handleMinOccupancyPage(TransactionId, HashMap, BTreePage)
	 */
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) 
			throws DbException, IOException, TransactionAbortedException {
		HashMap<PageId, Page> dirtypages = new HashMap<PageId, Page>();

		BTreePageId pageId = new BTreePageId(tableid, t.getRecordId().getPageId().pageNumber(), 
				BTreePageId.LEAF);
		BTreeLeafPage page = (BTreeLeafPage) getPage(tid, dirtypages, pageId, Permissions.READ_WRITE);
		page.deleteTuple(t);

		// if the page is below minimum occupancy, get some tuples from its siblings
		// or merge with one of the siblings
		int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples()/2; // ceiling
		if(page.getNumEmptySlots() > maxEmptySlots) { 
			//handleMinOccupancyPage(tid, dirtypages, page);
		}

		ArrayList<Page> dirtyPagesArr = new ArrayList<Page>();
		dirtyPagesArr.addAll(dirtypages.values());
		return dirtyPagesArr;
	}
	
	public void PrintStructure(TransactionId tid, HashMap<PageId, Page> dirtypages,BTreePageId pid /*root*/,int level) throws DbException, TransactionAbortedException{
		String s = "";
		for (int i=0;i<level;i++) s+= "\t";
 		
 		if (pid.pgcateg() == BTreePageId.INTERNAL){
 			BLinkTreeInternalPage p = (BLinkTreeInternalPage) getPage(tid, dirtypages, pid, Permissions.READ_ONLY);
 			Iterator<BTreeEntry> it = p.iterator();
 			System.out.println(s + "INTERNAL" +" pgNo: "+pid.pageNumber() + " numKeys: "+p.getNumEntries() +" empty: "+p.getNumEmptySlots() +" right "+( p.getRightSiblingId()==null?"null" :  p.getRightSiblingId().pageNumber()) +" highkey " + p.getHighKey() );
 			BTreeEntry e = null;
 			while (it.hasNext()){
 				e = it.next();			
 				PrintStructure(tid, dirtypages, e.getLeftChild(), level+1);
 				System.out.println(s+"Key: "+e.getKey().toString());
 				//PrintStructure(tid, dirtypages, e.getRightChild(), level+1);
 			}
 			//System.out.println(s+"Right Child");
 			PrintStructure(tid, dirtypages, e.getRightChild(), level+1);
 		} else {
 			BLinkTreeLeafPage p = (BLinkTreeLeafPage) getPage(tid, dirtypages, pid, Permissions.READ_ONLY);
 			System.out.println(s + "LEAF" +" pgNo: "+pid.pageNumber() +" right "+( p.getRightSiblingId()==null?"null" :  p.getRightSiblingId().pageNumber()) + " highKey " + p.getHighKey());
 			DumpLeafPage(p, s);
 		}
	}
	public void DumpLeafPage(BTreeLeafPage page, String prefix){
		String s = "[";
		Iterator<Tuple> it = page.iterator();
		while (it.hasNext())
			s+= it.next().getField(keyField) +",";
		s += "]";
		System.out.println(prefix +" "+page.getId() + " size: "+ page.getNumTuples()+" " + s);
	}
	public void DumpLeafPageId(int pageNo){
		System.out.println("Page "+pageNo +" Dump");
		try {
			BLinkTreeLeafPage p = (BLinkTreeLeafPage) getPage(new TransactionId(), new HashMap<PageId,Page>(), new BTreePageId(tableid, pageNo, BTreePageId.LEAF),Permissions.READ_ONLY );
			DumpLeafPage(p,"");
		} catch (DbException | TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Get a read lock on the root pointer page. Create the root pointer page and root page
	 * if necessary.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages 
	 * @return the root pointer page
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	BTreeRootPtrPage getRootPtrPage(TransactionId tid, HashMap<PageId, Page> dirtypages) throws DbException, IOException, TransactionAbortedException {
		synchronized(this) {
			if(f.length() == 0) {
				// create the root pointer page and the root page
				BufferedOutputStream bw = new BufferedOutputStream(
						new FileOutputStream(f, true));
				byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
				byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
				bw.write(emptyRootPtrData);
				bw.write(emptyLeafData);
				bw.close();
			}
		}

		// get a read lock on the root pointer page
		return (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.NO_LOCK);
	}


	/**
	 * get the specified tuples from the file based on its IndexPredicate value on
	 * behalf of the specified transaction. This method will acquire a read lock on
	 * the affected pages of the file, and may block until the lock can be
	 * acquired.
	 * 
	 * @param tid - the transaction id
	 * @param ipred - the index predicate value to filter on
	 * @return an iterator for the filtered tuples
	 */
	public DbFileIterator indexIterator(TransactionId tid, IndexPredicate ipred) {
		return new BLinkTreeSearchIterator(this, tid, ipred);
	}

	/**
	 * Get an iterator for all tuples in this B+ tree file in sorted order. This method 
	 * will acquire a read lock on the affected pages of the file, and may block until 
	 * the lock can be acquired.
	 * 
	 * @param tid - the transaction id
	 * @return an iterator for all the tuples in this file
	 */
	public DbFileIterator iterator(TransactionId tid) {
		return new BLinkTreeFileIterator(this, tid);
	}

}

/**
 * Helper class that implements the Java Iterator for tuples on a BTreeFile
 */
class BLinkTreeFileIterator extends AbstractDbFileIterator {

	Iterator<Tuple> it = null;
	BTreeLeafPage curp = null;

	TransactionId tid;
	BLinkTreeFile f;

	/**
	 * Constructor for this iterator
	 * @param f - the BTreeFile containing the tuples
	 * @param tid - the transaction id
	 */
	public BLinkTreeFileIterator(BLinkTreeFile f, TransactionId tid) {
		this.f = f;
		this.tid = tid;
	}

	/**
	 * Open this iterator by getting an iterator on the first leaf page
	 */
	public void open() throws DbException, TransactionAbortedException {
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
		BTreePageId root = rootPtr.getRootId();
		curp = f.findLeafPage(tid, root, Permissions.READ_ONLY, null);
		it = curp.iterator();
	}

	/**
	 * Read the next tuple either from the current page if it has more tuples or
	 * from the next page by following the right sibling pointer.
	 * 
	 * @return the next tuple, or null if none exists
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		if (it != null && !it.hasNext())
			it = null;

		while (it == null && curp != null) {
			BTreePageId nextp = curp.getRightSiblingId();
			if(nextp == null) {
				curp = null;
			}
			else {
				curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
						nextp, Permissions.READ_ONLY);
				it = curp.iterator();
				if (!it.hasNext())
					it = null;
			}
		}

		if (it == null)
			return null;
		return it.next();
	}

	/**
	 * rewind this iterator back to the beginning of the tuples
	 */
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	/**
	 * close the iterator
	 */
	public void close() {
		super.close();
		it = null;
		curp = null;
	}
}

/**
 * Helper class that implements the DbFileIterator for search tuples on a
 * B+ Tree File
 */
class BLinkTreeSearchIterator extends AbstractDbFileIterator {

	Iterator<Tuple> it = null;
	BTreeLeafPage curp = null;

	TransactionId tid;
	BLinkTreeFile f;
	IndexPredicate ipred;

	/**
	 * Constructor for this iterator
	 * @param f - the BTreeFile containing the tuples
	 * @param tid - the transaction id
	 * @param ipred - the predicate to filter on
	 */
	public BLinkTreeSearchIterator(BLinkTreeFile f, TransactionId tid, IndexPredicate ipred) {
		this.f = f;
		this.tid = tid;
		this.ipred = ipred;
	}

	/**
	 * Open this iterator by getting an iterator on the first leaf page applicable
	 * for the given predicate operation
	 */
	public void open() throws DbException, TransactionAbortedException {
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
		BTreePageId root = rootPtr.getRootId();
		if(ipred.getOp() == Op.EQUALS || ipred.getOp() == Op.GREATER_THAN 
				|| ipred.getOp() == Op.GREATER_THAN_OR_EQ) {
			curp = f.findLeafPage(tid, root, Permissions.READ_ONLY, ipred.getField());
		}
		else {
			curp = f.findLeafPage(tid, root, Permissions.READ_ONLY, null);
		}
		it = curp.iterator();
	}

	/**
	 * Read the next tuple either from the current page if it has more tuples matching
	 * the predicate or from the next page by following the right sibling pointer.
	 * 
	 * @return the next tuple matching the predicate, or null if none exists
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException,
	NoSuchElementException {
		while (it != null) {

			while (it.hasNext()) {
				Tuple t = it.next();
				if (t.getField(f.keyField()).compare(ipred.getOp(), ipred.getField())) {
					return t;
				}
				else if(ipred.getOp() == Op.LESS_THAN || ipred.getOp() == Op.LESS_THAN_OR_EQ) {
					// if the predicate was not satisfied and the operation is less than, we have
					// hit the end
					return null;
				}
				else if(ipred.getOp() == Op.EQUALS && 
						t.getField(f.keyField()).compare(Op.GREATER_THAN, ipred.getField())) {
					// if the tuple is now greater than the field passed in and the operation
					// is equals, we have reached the end
					return null;
				}
			}

			BTreePageId nextp = curp.getRightSiblingId();
			// if there are no more pages to the right, end the iteration
			if(nextp == null) {
				return null;
			}
			else {
				curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
						nextp, Permissions.READ_ONLY);
				it = curp.iterator();
			}
		}

		return null;
	}

	/**
	 * rewind this iterator back to the beginning of the tuples
	 */
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	/**
	 * close the iterator
	 */
	public void close() {
		super.close();
		it = null;
	}
}
