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
import diskmgr.Page;
import global.*;
import heap.InvalidSlotNumberException;
import heap.Tuple;

public class BTLeafPage extends BTSortedPage {
	public int keyType;

	public BTLeafPage(PageId arg0, int arg1) throws ConstructPageException,
			IOException {
		super(arg0, arg1);
		// TODO Auto-generated constructor stub
		keyType = arg1;
		this.setType(NodeType.LEAF);
	}

	public BTLeafPage(Page page, int arg1) throws ConstructPageException,
			IOException {
		super(arg1);
		keyType = arg1;
		this.setType(NodeType.LEAF);
	}

	public BTLeafPage(int arg1) throws ConstructPageException, IOException {
		super(arg1);
		keyType = arg1;
		this.setType(NodeType.LEAF);
	}

	public boolean contains(KeyClass key) throws ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, ConstructPageException,
			HashEntryNotFoundException {
		BTSortedPage hfp = new BTSortedPage(this.getCurPage(), keyType);
		RID rid = hfp.firstRecord();
		while (rid != null) {
			Tuple tuple = new Tuple();
			tuple = hfp.getRecord(rid);
			KeyDataEntry kde = BT.getEntryFromBytes(tuple.getTupleByteArray(),
					tuple.getOffset(), tuple.getLength(), keyType,
					NodeType.LEAF);
			if (BT.keyCompare(kde.key, key) == 0) {
				SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), false);
				return true;
			} else {
				if (hfp.nextRecord(rid) != null)
					rid.copyRid(hfp.nextRecord(rid));
				else
					break;
			}
		}
		SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), false);
		return false;
	}

	public RID insertRecord(KeyClass key, RID dataRid)
			throws ReplacerException, HashOperationException,
			PageUnpinnedException, InvalidFrameNumberException,
			PageNotReadException, BufferPoolExceededException,
			PagePinnedException, BufMgrException, IOException,
			KeyNotMatchException, NodeNotMatchException, ConvertException,
			HashEntryNotFoundException, ConstructPageException {
		BTSortedPage hfp = new BTSortedPage(this.getCurPage(), keyType);
		KeyDataEntry kde;
		kde = new KeyDataEntry(key, dataRid);
		byte[] arr = BT.getBytesFromEntry(kde);
		RID toret = hfp.insertRecord(arr);
		if (toret == null)
			SystemDefs.JavabaseBM.unpinPage(hfp.getCurPage(), false);
		else
			SystemDefs.JavabaseBM.unpinPage(hfp.getCurPage(), true);
		return toret;
	}

	public KeyDataEntry getFirst(RID rid) throws ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException,
			HashEntryNotFoundException, ConstructPageException {
		BTSortedPage hfp = new BTSortedPage(this.getCurPage(), keyType);
		RID firstRid = hfp.firstRecord();
		if (firstRid == null)
			return null;
		Tuple tuple = new Tuple();
		tuple = hfp.getRecord(firstRid);
		KeyDataEntry kde = BT.getEntryFromBytes(tuple.getTupleByteArray(),
				tuple.getOffset(), tuple.getLength(), keyType, NodeType.LEAF);
		rid.copyRid(firstRid);
		SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), false);
		return kde;
	}

	public KeyDataEntry getNext(RID rid) throws ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException,
			HashEntryNotFoundException, ConstructPageException {
		BTSortedPage hfp = new BTSortedPage(this.getCurPage(), keyType);
		RID rambo = hfp.nextRecord(rid);
		if (rambo != null)
			rid.copyRid(rambo);
		else
			rid = null;
		Tuple tuple = new Tuple();
		tuple = hfp.getRecord(rambo);
		KeyDataEntry kde = BT.getEntryFromBytes(tuple.getTupleByteArray(),
				tuple.getOffset(), tuple.getLength(), keyType,
				NodeType.LEAF);
		SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), false);
		return kde;
	}

	public KeyDataEntry getCurrent(RID rid) throws ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException,
			HashEntryNotFoundException, ConstructPageException {
		BTSortedPage hfp = new BTSortedPage(this.getCurPage(), keyType);
		Tuple tuple = new Tuple();
		tuple = hfp.getRecord(rid);
		KeyDataEntry kde = BT.getEntryFromBytes(tuple.getTupleByteArray(),
				tuple.getOffset(), tuple.getLength(), keyType,
				NodeType.LEAF);
		SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), false);
		return kde;
	}

	public boolean delEntry(KeyDataEntry dEntry) throws ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException,
			HashEntryNotFoundException, ConstructPageException,
			DeleteRecException {
		BTSortedPage hfp = new BTSortedPage(this.getCurPage(), keyType);
		RID firstRid = hfp.firstRecord();
		while (firstRid != null) {
			Tuple tuple = hfp.getRecord(firstRid);
			KeyDataEntry kde = BT.getEntryFromBytes(tuple.getTupleByteArray(),
					tuple.getOffset(), tuple.getLength(), keyType,
					NodeType.LEAF);
			if (BT.keyCompare(kde.key, dEntry.key) == 0) {
				hfp.deleteSortedRecord(firstRid);
				SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), true);
				return true;
			}
			if (hfp.nextRecord(firstRid) != null)
				firstRid.copyRid(hfp.nextRecord(firstRid));
			else
			{
				SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), false);
				return false;
			}
		}
		SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), false);
		return false;
	}

	public int pageSize() throws ReplacerException, HashOperationException,
			PageUnpinnedException, InvalidFrameNumberException,
			PageNotReadException, BufferPoolExceededException,
			PagePinnedException, BufMgrException, IOException,
			InvalidSlotNumberException, HashEntryNotFoundException,
			ConstructPageException {
		BTSortedPage hfp = new BTSortedPage(this.getCurPage(), keyType);
		RID iterator = hfp.firstRecord();
		int size = 0;
		while (iterator != null) {
			Tuple tuple = hfp.getRecord(iterator);
			size += tuple.getLength();
			if (hfp.nextRecord(iterator) != null)
				iterator = (hfp.nextRecord(iterator));
			else
				break;
		}
		SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), false);
		return size;
	}
}
