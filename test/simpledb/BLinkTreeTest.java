package simpledb;

import simpledb.Predicate.Op;
import simpledb.systemtest.SimpleDbTestBase;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.JUnit4TestAdapter;

class DbTransaction extends Thread {

	TransactionId tid = new TransactionId();
	int id;
	boolean delete = false;
	//BLinkTreeFile b;
	BTreeFile b;
	@Override
	public void run() {
		System.out.println("Tx "+tid.getId() +" Start on thread "+this.getName() + " id "+id);
		if (!delete) {
			for (int i=0;i<1;i++){
				insertTuple();
			}
		} else {
			for (int i=0;i<1;i++){
				findTuple();
				
			}
		}
		try {
			Database.getBufferPool().transactionComplete(tid);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	ArrayList<Page> insertTuple(){
		Tuple t = new Tuple(b.getTupleDesc());
		int val = new Random().nextInt(512);
		t.setField(0, new IntField(val) );
		t.setField(1, new IntField(val));
		//System.out.println(tid + " "+val);
		ArrayList<Page> dirty = null;
		try {
			dirty = b.insertTuple( tid, t);
		} catch (DbException dbe){
			dbe.printStackTrace();
			System.exit(0);
		} catch (IOException | TransactionAbortedException e) {
			//e.printStackTrace();
			try {
				Database.getBufferPool().transactionComplete(tid,false);
				DbTransaction rep = new DbTransaction();
				rep.b = b;
				rep.id = id;
				//rep.start();
				//rep.join();
				this.stop();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} 
			
			//System.exit(0);
		}
		
		for (Page p: dirty){
			p.markPageDirty(true, tid);
		}
		return dirty;
	}
	
	boolean findTuple(){
		DbFileIterator it =b.indexIterator(tid, new IndexPredicate(Op.EQUALS, new IntField(new Random().nextInt(512))));
		try {
			it.open();
			if (it.hasNext()){
				Tuple t = it.next();
				//System.out.println("Tx "+Thread.currentThread().getId() +" "+t + " "+ t.getRecordId().tupleno());
				if (t.getRecordId() != null)b.deleteTuple(tid, t);
			}
		} catch (DbException e) {
			// TODO Auto-generated catch block
			//System.out.println("Tx "+Thread.currentThread().getId()+ " "+e.getMessage());
			//e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			try {
				Database.getBufferPool().transactionComplete(tid,false);
				DbTransaction rep = new DbTransaction();
				rep.b = b;
				rep.id = id;
				//rep.start();
				//rep.join();
				this.stop();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} 
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
}

public class BLinkTreeTest extends SimpleDbTestBase {
	private TransactionId tid;
	private TupleDesc td;
	
	private HashMap<PageId, Page>  dirtypages = new HashMap<PageId, Page>();

	/**
	 * Set up initial resources for each unit test.
	 */
	@Before
	public void setUp() throws Exception {
		//f = BTreeUtility.createRandomBTreeFile(2, 20, null, null, 0);
		td = Utility.getTupleDesc(2);
		tid = new TransactionId();
	}

	@After
	public void tearDown() throws Exception {
		Database.getBufferPool().transactionComplete(tid);
	}


	
	public int PrintTree(BTreeFile b){
		try {
			BufferPool.DEBUG_ON = false;
			System.out.println("Printing Tree");
			TransactionId tid = new TransactionId();
			BTreePageId rootid = b.getRootPtrPage(tid, dirtypages).getRootId();
			System.out.println("Root "+ rootid);
			int nT =b.PrintStructure(tid, dirtypages, rootid, 0);
			Database.getBufferPool().transactionComplete(tid);
			return nT;
		} catch (DbException | IOException | TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
			
		}
	}
	
	@Test
	public void Test() throws Exception {    	
		File f = File.createTempFile("tmp", "txt");
		f.deleteOnExit();
		BufferPool.setPageSize(1000);
		BLinkTreeFile b = new BLinkTreeFile( f, 0 , td);
		Database.getCatalog().addTable(b);
		//PrintTree(b);
		for (int i=0;i<5014;i++){
			if (i%10==0) {
				Database.getBufferPool().transactionComplete(tid);
				tid = new TransactionId();
			}
			Tuple t = new Tuple(td);
			t.setField(0, new IntField( (i*4567) %512));
			t.setField(1, new IntField(i));
			//System.out.println(t);
			ArrayList<Page> dirty =b.insertTuple( tid, t);
			for (Page p: dirty){
				p.markPageDirty(true, tid);
			}
		}
		System.out.println("NumPages "+b.numPages());
		PrintTree(b);
		//BTreeChecker.checkRep(b, tid, dirtypages, false);
	
	}
	@Test
	public void TransTest() throws Exception {   
		BufferPool.DEBUG_ON = false;
		File f = File.createTempFile("tmp", "txt");
		f.deleteOnExit();
		BufferPool.setPageSize(1000);
		
		BLinkTreeFile b = new BLinkTreeFile( f, 0 , td);
		//BTreeFile b = new BTreeFile( f, 0 , td);
		Database.getCatalog().addTable(b);
		BufferPool.DETECT_DEADLOCK = !(b instanceof BLinkTreeFile);
		//PrintTree(b);
		for (int i=0;i<1014;i++){
			if (i%10==0) {
				Database.getBufferPool().transactionComplete(tid);
				tid = new TransactionId();
			}
			Tuple t = new Tuple(td);
			t.setField(0, new IntField( (i*4567) %512));
			t.setField(1, new IntField(i));
			//System.out.println(t);
			ArrayList<Page> dirty =b.insertTuple( tid, t);
			for (Page p: dirty){
				p.markPageDirty(true, tid);
			}
		}
		Database.getBufferPool().transactionComplete(tid);
		
		PrintTree(b);
		
		BufferPool.DEBUG_ON = false;
		long time = System.nanoTime();
		
		final int NUM_TRANS = 4000;
		//final int NUM_TRANS = 1;

		DbTransaction[] tx = new DbTransaction[NUM_TRANS];
		for (int i=0;i<NUM_TRANS;i++){
			tx[i]= new DbTransaction();
			tx[i].b = b;
			tx[i].id = i;
			if ( (i%2)==0)
				tx[i].delete  = true; 
			tx[i].start();
			//tx[i].join();
			//if (i%10 == 0 )Thread.sleep(100);
			
		}
		for (Thread t: tx){
			t.join();
		}
		long time1 = System.nanoTime();
		//Thread.sleep(2000);
		
		int nT =PrintTree(b);
		System.out.println("NumPages "+b.numPages() +" numTuples "+nT + " ratio " + ((nT+0.0)/b.numPages()));
		System.out.println();
		System.out.println("Time "+(time1-time)/1000000);
		//BTreeChecker.checkRep(b, tid, dirtypages, false);
	
	}


	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BLinkTreeTest.class);
	}
}
