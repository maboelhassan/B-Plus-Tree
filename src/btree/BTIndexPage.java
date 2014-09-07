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
import diskmgr.Page;

public class BTIndexPage extends BTSortedPage {
	public int keyType;

	public BTIndexPage(int arg0) throws ConstructPageException, IOException {
		super(arg0);
		this.setType(NodeType.INDEX);
		keyType = arg0;
	}

	public BTIndexPage(PageId id, int arg) throws ConstructPageException,
			IOException {
		super(id, arg);
		this.setType(NodeType.INDEX);
		keyType = arg;
	}

	public BTIndexPage(Page page, int arg) throws IOException {
		super(page, arg);
		this.setType(NodeType.INDEX);
		keyType = arg;
	}

	public KeyDataEntry getFirst(RID rid) throws ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException,
			HashEntryNotFoundException, ConstructPageException {
		BTSortedPage hfp = new BTSortedPage(this.getCurPage(), keyType);
		RID temp = hfp.firstRecord();
		if(temp!=null)
			rid.copyRid(temp);
		else
			rid = null;
		KeyDataEntry kde;
		Tuple tu = new Tuple();
		tu = hfp.getRecord(temp);
		kde = BT.getEntryFromBytes(tu.getTupleByteArray(), tu.getOffset(),
				tu.getLength(), keyType, NodeType.INDEX);
		SystemDefs.JavabaseBM.unpinPage(getCurPage(), false);
		return kde;
	}

	public RID insertKey(KeyClass data, PageId pid)
			throws KeyNotMatchException, NodeNotMatchException,
			ConvertException, IOException, ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			HashEntryNotFoundException, ConstructPageException {
		BTSortedPage hfp = new BTSortedPage(this.getCurPage(), keyType);
		KeyDataEntry kde = new KeyDataEntry(data, pid);
		byte[] arr = BT.getBytesFromEntry(kde);
		RID rid = hfp.insertRecord(arr);
		if (rid.equals(null))
			SystemDefs.JavabaseBM.unpinPage(hfp.getCurPage(), false);
		else
			SystemDefs.JavabaseBM.unpinPage(hfp.getCurPage(), true);
		return rid;
	}

	public PageId getPageN0ByKey(KeyClass data) throws ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException,
			HashEntryNotFoundException, ConstructPageException {
		BTSortedPage hfp = new BTSortedPage(this.getCurPage(), keyType);
		RID temp = hfp.firstRecord();
		while (temp!=null) {
			Tuple tu = new Tuple();
			tu = hfp.getRecord(temp);
			KeyDataEntry kde = BT.getEntryFromBytes(tu.getTupleByteArray(),
					tu.getOffset(), tu.getLength(), keyType, NodeType.INDEX);

			KeyClass comparing = kde.key;

			if (BT.keyCompare(comparing, data) == 0) {
				SystemDefs.JavabaseBM.unpinPage(hfp.getCurPage(), false);
				return (PageId) ((IndexData) kde.data).getData();
			}
			if(hfp.nextRecord(temp) != null)
			{
				temp.copyRid(hfp.nextRecord(temp));
			}else
				temp = null;
		}
		SystemDefs.JavabaseBM.unpinPage(hfp.getCurPage(), false);
		return null;
	}

	public KeyDataEntry getNext(RID rid) throws ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException,
			HashEntryNotFoundException, ConstructPageException {
		BTSortedPage hfp = new BTSortedPage(this.getCurPage(), keyType);
		RID iterator = new RID();
		iterator.copyRid(rid);
		if(rid == null)
		{
			SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), false);
			return null;
		}
		iterator=hfp.nextRecord(iterator);
		if (iterator==null) {
			SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), false);
			rid = null;
			return null;
		}
		Tuple tu = new Tuple();
		tu = hfp.getRecord(iterator);
		KeyDataEntry kde = BT.getEntryFromBytes(tu.getTupleByteArray(), tu.getOffset(),
				tu.getLength(), keyType, NodeType.INDEX);
		rid.copyRid(iterator);
		SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), false);
		return kde;
	}

	public void setLeftLink(PageId left) throws IOException {
		this.setPrevPage(left);
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
		int num = 0;
		while (iterator!=(null)) {
			num++;
			Tuple tuple = hfp.getRecord(iterator);
			size += tuple.getLength();
			iterator=(hfp.nextRecord(iterator));
		}
		SystemDefs.JavabaseBM.unpinPage(this.getCurPage(), false);
		return size;
	}

	public PageId getLeftLink() throws IOException {
		return this.getPrevPage();
	}
}
