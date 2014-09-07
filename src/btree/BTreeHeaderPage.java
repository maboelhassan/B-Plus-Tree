package btree;

import java.io.IOException;

import global.PageId;
import heap.HFPage;

public class BTreeHeaderPage extends HFPage {

	int keySize;
	int keyType;

	public BTreeHeaderPage(PageId id, int _keySize, int _keyType)
			throws IOException {

		this.keySize = _keySize;
		this.keyType = _keyType;
		this.setNextPage(id);

	}

	public PageId get_rootId() throws IOException {
		return this.getNextPage();
	}

	public void setPointer(PageId pid) throws IOException {
		this.setNextPage(pid);
	}

	public short get_keyType() {
		return (short) keyType;
	}
}
