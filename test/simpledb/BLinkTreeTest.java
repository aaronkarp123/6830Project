package simpledb;

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
	//BLinkTreeFile b;
	BTreeFile b;
	@Override
	public void run() {
		System.out.println("Tx "+tid.getId() +" Start on thread "+this.getName());
		for (int i=0;i<20;i++){
			Tuple t = new Tuple(b.getTupleDesc());
			int val = ((i*4567) %51)*10 + id;
			t.setField(0, new IntField(val) );
			t.setField(1, new IntField(i));
			//System.out.println(t);
			ArrayList<Page> dirty = null;
			try {
				dirty = b.insertTuple( tid, t);
			} catch (DbException | IOException | TransactionAbortedException e) {
				e.printStackTrace();
				//System.exit(0);
			}
			for (Page p: dirty){
				p.markPageDirty(true, tid);
			}
		}
		try {
			Database.getBufferPool().transactionComplete(tid);
		} catch (IOException e) {
			e.printStackTrace();
		}
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


	
	public void PrintTree(BTreeFile b){
		try {
			
			BTreePageId rootid = b.getRootPtrPage(tid, dirtypages).getRootId();
			System.out.println("Root "+ rootid);
			b.PrintStructure(tid, dirtypages, rootid, 0);
		} catch (DbException | IOException | TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		File f = File.createTempFile("tmp", "txt");
		f.deleteOnExit();
		BufferPool.setPageSize(1000);
		//BLinkTreeFile b = new BLinkTreeFile( f, 0 , td);
		BTreeFile b = new BTreeFile( f, 0 , td);
		Database.getCatalog().addTable(b);
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
		
		final int NUM_TRANS = 1000;
		
		DbTransaction[] tx = new DbTransaction[NUM_TRANS];
		for (int i=0;i<NUM_TRANS;i++){
			tx[i]= new DbTransaction();
			tx[i].b = b;
			tx[i].id = i;
			tx[i].start();
			
		}
		for (Thread t: tx){
			t.join();
		}
		long time1 = System.nanoTime();
		//Thread.sleep(2000);
		System.out.println("NumPages "+b.numPages());
		PrintTree(b);
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
