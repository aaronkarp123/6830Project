package simpledb;

import simpledb.systemtest.SimpleDbTestBase;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.JUnit4TestAdapter;

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
		BTreeFile b = new BTreeFile( f, 0 , td);
		Database.getCatalog().addTable(b);
		//PrintTree(b);
		for (int i=0;i<1000;i++){
			if (i%10000==0) {
				Database.getBufferPool().transactionComplete(tid);
				tid = new TransactionId();
			}
			Tuple t = new Tuple(td);
			t.setField(0, new IntField(i));
			t.setField(1, new IntField(i));
			//System.out.println(t);
			ArrayList<Page> dirty =b.insertTuple( tid, t);
			for (Page p: dirty){
				p.markPageDirty(true, tid);
			}
		}
		System.out.println("NumPages "+b.numPages());
		PrintTree(b);
		BTreeChecker.checkRep(b, tid, dirtypages, false);
		
	}
	


	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BLinkTreeTest.class);
	}
}
