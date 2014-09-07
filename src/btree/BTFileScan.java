package btree;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.InvalidSlotNumberException;
import heap.Tuple;

public class BTFileScan {
	Pair lowPair;
	Pair hiPair;
	boolean first = true;
	Pair current;
	int keyType;

	public BTFileScan() {
		lowPair = new Pair();
		current = new Pair();
		hiPair = new Pair();
	}

	protected BTFileScan(Pair lowPair, Pair upPair, int ktype) {
		this.lowPair = new Pair(lowPair.pairPageId, lowPair.paiRid);
		current = new Pair(lowPair.pairPageId, lowPair.paiRid);
		this.hiPair = new Pair(upPair.pairPageId, upPair.paiRid);
		keyType = ktype;
	}

	private Pair advance() throws ConstructPageException, ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, HashEntryNotFoundException {
		BTSortedPage btsp = new BTSortedPage(keyType);
		SystemDefs.JavabaseBM.pinPage(current.pairPageId, btsp, true);
		RID check = btsp.nextRecord(current.paiRid);
		if (check.equals(null)) {
			PageId next = btsp.getNextPage();
			BTSortedPage examine = new BTSortedPage(keyType);
			SystemDefs.JavabaseBM.pinPage(next, examine, true);
			RID newRID = examine.firstRecord();
			SystemDefs.JavabaseBM.unpinPage(current.pairPageId, false);
			current = new Pair(next, newRID);
			SystemDefs.JavabaseBM.unpinPage(next, false);
			return current;
		} else {
			current = new Pair(current.pairPageId, check);
			SystemDefs.JavabaseBM.unpinPage(current.pairPageId, false);
			return current;
		}
	}

	public KeyDataEntry get_next() throws ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, ConstructPageException, InvalidSlotNumberException,
			KeyNotMatchException, NodeNotMatchException, ConvertException,
			HashEntryNotFoundException {
		if (first) {
			first = false;
			BTSortedPage btsp = new BTSortedPage(keyType);
			SystemDefs.JavabaseBM.pinPage(lowPair.pairPageId, btsp, true);
			Tuple t = btsp.getRecord(lowPair.paiRid);
			KeyDataEntry kde = BT.getEntryFromBytes(btsp.getpage(),
					t.getOffset(), t.getLength(), keyType, NodeType.LEAF);
			SystemDefs.JavabaseBM.unpinPage(lowPair.pairPageId, false);
			return kde;
		} else {
			current = advance();
			BTSortedPage btsp = new BTSortedPage(keyType);
			SystemDefs.JavabaseBM.pinPage(current.pairPageId, btsp, true);
			Tuple t = btsp.getRecord(current.paiRid);
			KeyDataEntry kde = BT.getEntryFromBytes(btsp.getpage(),
					t.getOffset(), t.getLength(), keyType, NodeType.LEAF);
			SystemDefs.JavabaseBM.unpinPage(lowPair.pairPageId, false);
			return kde;
		}
	}

	public void delete_current() throws ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, ConstructPageException, InvalidSlotNumberException,
			KeyNotMatchException, NodeNotMatchException, ConvertException,
			DeleteRecException {
		BTSortedPage btsp = new BTSortedPage(keyType);
		SystemDefs.JavabaseBM.pinPage(current.pairPageId, btsp, true);
		btsp.deleteSortedRecord(current.paiRid);
	}

	public int keysize() throws ConstructPageException, ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, HashEntryNotFoundException {
		BTSortedPage btsp = new BTSortedPage(keyType);
		SystemDefs.JavabaseBM.pinPage(current.pairPageId, btsp, true);
		Tuple t = btsp.getRecord(current.paiRid);
		SystemDefs.JavabaseBM.unpinPage(lowPair.pairPageId, false);
		return t.getLength();
	}
	
	
	
	

}
