package btree;

import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;
import java.util.Stack;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidBufferException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotFoundException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.DiskMgrException;
import diskmgr.DuplicateEntryException;
import diskmgr.FileEntryNotFoundException;
import diskmgr.FileIOException;
import diskmgr.FileNameTooLongException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;

public class BTreeFile extends IndexFile {

	BTreeHeaderPage headerPage;
	static int counter = 0;
	boolean open;
	String dbname;

	public BTreeFile(String filepath) throws FileIOException,
			InvalidPageNumberException, DiskMgrException, IOException {

		PageId tempId = SystemDefs.JavabaseDB.get_file_entry(filepath);
		if (tempId.pid == -1) {

		} else {

		}

	}

	public BTreeFile(String fileName, int type, int sizeOfKey, int arg)
			throws FileIOException, InvalidPageNumberException,
			DiskMgrException, IOException, BufferPoolExceededException,
			HashOperationException, ReplacerException,
			HashEntryNotFoundException, InvalidFrameNumberException,
			PagePinnedException, PageUnpinnedException, PageNotReadException,
			BufMgrException, FileNameTooLongException, InvalidRunSizeException,
			DuplicateEntryException, OutOfSpaceException,
			ConstructPageException {

		PageId temPageId = new PageId();
		try {
			temPageId
					.copyPageId(SystemDefs.JavabaseDB.get_file_entry(fileName));
			headerPage = new BTreeHeaderPage(temPageId, sizeOfKey, type);
			SystemDefs.JavabaseDB.add_file_entry(fileName,
					headerPage.get_rootId());
		} catch (Exception e) {
			open = true;
			dbname = fileName;
			// keda b3d elclose elBT ra7et 3shan el header pointer msh hytkatab
			headerPage = new BTreeHeaderPage(new PageId(-1), sizeOfKey, type);
			PageId newPiId = SystemDefs.JavabaseBM.newPage(headerPage, 1);
			headerPage.init(newPiId, headerPage);
		}
	}

	private BTSortedPage emptyPage(BTSortedPage btlp) throws IOException,
			InvalidSlotNumberException {
		btlp.init(btlp.getCurPage(), btlp);
		// RID iterator = btlp.firstRecord();
		// while (iterator != null) {
		// btlp.deleteRecord(iterator);
		// iterator = (btlp.nextRecord(iterator));
		// }
		return btlp;
	}

	private void firstInsert(KeyClass data, RID rid)
			throws ConstructPageException, ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, HashEntryNotFoundException, DiskMgrException,
			InsertRecException, InvalidSlotNumberException,
			KeyNotMatchException, NodeNotMatchException, ConvertException {
		BTIndexPage btsp = new BTIndexPage(headerPage.keyType);
		headerPage.setPointer(btsp.getCurPage());
		BTLeafPage btl = new BTLeafPage(headerPage.keyType);
		btsp.setLeftLink(btl.getCurPage());
		BTLeafPage btls = new BTLeafPage(headerPage.keyType);
		KeyDataEntry rooted = new KeyDataEntry(data, btls.getCurPage());
		btsp.insertRecord(rooted);
		btls.insertRecord(new KeyDataEntry(data, rid));
		btl.setNextPage(btls.getCurPage());
		btls.setPrevPage(btl.getCurPage());
		SystemDefs.JavabaseBM.unpinPage(headerPage.get_rootId(), true);
		SystemDefs.JavabaseBM.unpinPage(btl.getCurPage(), true);
		SystemDefs.JavabaseBM.unpinPage(btls.getCurPage(), true);
	}

	@Override
	public void insert(KeyClass data, RID rid) {
		// TODO Auto-generated method stub
		try {
			if (BT.getKeyLength(data) > headerPage.keySize) {
				System.err.println(" Invalid Key Insert");
			}
		} catch (KeyNotMatchException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		PageId leafPid = new PageId();
		try {
			KeyDataEntry toinsert = new KeyDataEntry(data, rid);
			Stack<Integer> path = getPaths(data, leafPid);
			// System.out.println(leafPid + " the coming leaf pid");
			// System.out.println(path.toString() + " the stack");
			if (path.size() == 1 && path.peek() == -1) {
				// first insert
				firstInsert(data, rid);
				return;
			} else {
				BTLeafPage btlp = new BTLeafPage(leafPid, headerPage.keyType);
				// SystemDefs.JavabaseBM.pinPage(leafPid, btlp, true);
				if (btlp.contains(data)) {
					// throw Exception
					SystemDefs.JavabaseBM.unpinPage(btlp.getCurPage(), false);
					System.err.println(" Invalid Input");
				} else {
					KeyDataEntry toCopyUp = null;
					RID indicator = btlp.insertRecord(data, rid);
					if (indicator == null) {
						// split w la ellah ela allah
						int totalSize = btlp.pageSize();
						byte arr[] = new byte[btlp.getpage().length];
						for (int i = 0; i < arr.length; i++) {
							arr[i] = btlp.getpage()[i];
						}
						BTLeafPage temp = new BTLeafPage(leafPid,
								headerPage.keyType);
						btlp = (BTLeafPage) emptyPage(btlp);
						for (int i = 0; i < arr.length; i++) {
							temp.getpage()[i] = arr[i];
						}
						BTLeafPage newPage = new BTLeafPage(headerPage.keyType);
						// start of hell
						RID iterating = temp.firstRecord();
						int insertedSize = 0;
						boolean newInsert = false;
						while (insertedSize < totalSize / 2) {
							Tuple tuple = temp.getRecord(iterating);
							KeyDataEntry current = BT.getEntryFromBytes(
									tuple.getTupleByteArray(),
									tuple.getOffset(), tuple.getLength(),
									headerPage.keyType, NodeType.LEAF);
							// check for invalid insertion
							// e7na mest5dmin method 8lt mn elbtleafPage
							if (BT.keyCompare(current.key, data) < 0
									&& !newInsert) {
								btlp.insertRecord(current);
								iterating.copyRid(temp.nextRecord(iterating));
								insertedSize += tuple.getLength();
								continue;
							} else if (!newInsert) {
								btlp.insertRecord(toinsert);
								newInsert = true;
								insertedSize += (BT.getKeyLength(toinsert.key) + BT
										.getDataLength(NodeType.LEAF));
								continue;
							} else if (newInsert) {
								btlp.insertRecord(current);
								if (temp.nextRecord(iterating) == null)
									break;
								iterating.copyRid(temp.nextRecord(iterating));
								insertedSize += tuple.getLength();
								continue;
							}
						}
						boolean first = true;
						while (iterating != null) {
							Tuple tuple = temp.getRecord(iterating);
							KeyDataEntry current = BT.getEntryFromBytes(
									tuple.getTupleByteArray(),
									tuple.getOffset(), tuple.getLength(),
									headerPage.keyType, NodeType.LEAF);
							if (BT.keyCompare(current.key, data) < 0
									&& !newInsert) {
								RID ins = newPage.insertRecord(current.key,
										(RID) ((LeafData) current.data)
												.getData());
								if (ins == null) {
									// throw Exception
								}
								if (first) {
									first = false;
									toCopyUp = new KeyDataEntry(current.key,
											newPage.getCurPage());
								}
								iterating = (temp.nextRecord(iterating));
							} else if (!newInsert) {
								RID ins = newPage.insertRecord(toinsert.key,
										(RID) ((LeafData) toinsert.data)
												.getData());
								if (ins == (null)) {
									// throw Exception
								}
								if (first) {
									first = false;
									toCopyUp = new KeyDataEntry(toinsert.key,
											newPage.getCurPage());
								}
							} else if (newInsert) {
								RID ins = newPage.insertRecord(current.key,
										(RID) ((LeafData) current.data)
												.getData());
								if (ins == (null)) {
									// throw Exception
								}
								if (first) {
									first = false;
									toCopyUp = new KeyDataEntry(current.key,
											newPage.getCurPage());
								}
								iterating = (temp.nextRecord(iterating));
							}

						}
						for (int i = 0; i < arr.length; i++) {
							temp.getpage()[i] = btlp.getpage()[i];
						}
						if (btlp.getNextPage().pid != -1) {
							HFPage tempo = new HFPage();
							// special case btlp.getNextPage() = -1;
							SystemDefs.JavabaseBM.pinPage(btlp.getNextPage(),
									tempo, true);
							newPage.setNextPage(btlp.getNextPage());
							btlp.setNextPage(newPage.getCurPage());
							newPage.setPrevPage(leafPid);
							tempo.setPrevPage(newPage.getCurPage());
							SystemDefs.JavabaseBM.unpinPage(tempo.getCurPage(),
									true);
							SystemDefs.JavabaseBM.unpinPage(leafPid, true);
							SystemDefs.JavabaseBM.unpinPage(leafPid, true);

							SystemDefs.JavabaseBM.unpinPage(
									newPage.getCurPage(), true);
							goUp(path, toCopyUp);
						} else {
							newPage.setNextPage(btlp.getNextPage());
							btlp.setNextPage(newPage.getCurPage());
							newPage.setPrevPage(leafPid);
							SystemDefs.JavabaseBM.unpinPage(leafPid, true);
							SystemDefs.JavabaseBM.unpinPage(leafPid, true);

							SystemDefs.JavabaseBM.unpinPage(
									newPage.getCurPage(), true);
							goUp(path, toCopyUp);
						}

						// tam b7md allah nehayet split elleaf w 2ela leqa2 a5ar
						// f
						// split elindex
					} else {
						SystemDefs.JavabaseBM.unpinPage(leafPid, true);
						return;
					}
				}
			}
		} catch (ConstructPageException | ReplacerException
				| HashOperationException | PageUnpinnedException
				| InvalidFrameNumberException | PageNotReadException
				| BufferPoolExceededException | PagePinnedException
				| BufMgrException | InvalidSlotNumberException
				| KeyNotMatchException | NodeNotMatchException
				| ConvertException | HashEntryNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DiskMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InsertRecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private Stack<Integer> getPaths(KeyClass data, PageId thePageId)
			throws ConstructPageException, ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, HashEntryNotFoundException {

		// e7na shaghaleen men gheer ma nbos 3la el awl link ;
		Stack<Integer> path = new Stack<Integer>();
		if (headerPage.get_rootId().pid == -1) {
			path.add(-1);
			return path;
		}
		BTSortedPage btsp = new BTSortedPage(headerPage.get_rootId(),
				headerPage.keyType);
		PageId yourpid = new PageId();
		yourpid.copyPageId(headerPage.get_rootId());
		PageId mypid = new PageId();
		mypid.copyPageId(headerPage.get_rootId());
		RID rid1 = new RID();
		RID rid2 = new RID();
		if (btsp.firstRecord() == null) {
			path.add(-1);
			return path;
		} else
			rid1.copyRid(btsp.firstRecord());
		if (btsp.nextRecord(rid1) != null)
			rid2.copyRid(btsp.nextRecord(rid1));
		else
			rid2 = null;
		while (btsp.getType() != NodeType.LEAF) {
			yourpid.copyPageId(mypid);
			path.add(btsp.getCurPage().pid);
			if (btsp.firstRecord() != null)
				rid1.copyRid(btsp.firstRecord());
			else
				continue;
			if (btsp.nextRecord(rid1) != null)
				rid2 = new RID(btsp.nextRecord(rid1).pageNo,
						btsp.nextRecord(rid1).slotNo);
			else
				rid2 = null;
			while (rid1 != null) {
				Tuple t1, t2;
				KeyDataEntry k1, k2;
				t1 = new Tuple();
				t2 = new Tuple();
				t1 = btsp.getRecord(rid1);
				if (rid2 == null) {
					t2 = null;
					k2 = null;

				} else {
					t2 = btsp.getRecord(rid2);
					k2 = BT.getEntryFromBytes(t2.getTupleByteArray(),
							t2.getOffset(), t2.getLength(), headerPage.keyType,
							NodeType.INDEX);
				}
				k1 = BT.getEntryFromBytes(t1.getTupleByteArray(),
						t1.getOffset(), t1.getLength(), headerPage.keyType,
						NodeType.INDEX);

				if (k2 == null) {
					// aro7 llpath bta3 k1
					if (rid1.equals(btsp.firstRecord())) {
						if (BT.keyCompare(k1.key, data) < 0) {
							mypid.copyPageId(btsp.getPrevPage());
							break;
						}
					}
					mypid.copyPageId((PageId) ((IndexData) k1.data).getData());
					break;
				} else if (BT.keyCompare(k1.key, data) >= 0
						&& BT.keyCompare(k2.key, data) < 0) {
					// hnro7 lPageID bta3et k1
					mypid.copyPageId((PageId) ((IndexData) k1.data).getData());
					break;
				} else {
					if (rid2 != null)
						rid1.copyRid(rid2);
					else
						break;
					if (btsp.nextRecord(rid1) != null)
						rid2.copyRid(btsp.nextRecord(rid1));
					else
						rid2 = null;
					continue;
				}
			}
			SystemDefs.JavabaseBM.unpinPage(yourpid, false);
			SystemDefs.JavabaseBM.pinPage(mypid, btsp, true);
		}
		SystemDefs.JavabaseBM.unpinPage(btsp.getCurPage(), false);
		thePageId.copyPageId(mypid);
		return path;
	}

	@Override
	public boolean Delete(KeyClass data, RID rid) {
		// TODO Auto-generated method stub
		KeyDataEntry toDelete = new KeyDataEntry(data, rid);
		try {
			return delete(headerPage.get_rootId(), toDelete);
		} catch (KeyNotMatchException | NodeNotMatchException
				| ConvertException | ReplacerException | HashOperationException
				| PageUnpinnedException | InvalidFrameNumberException
				| PageNotReadException | BufferPoolExceededException
				| PagePinnedException | BufMgrException
				| ConstructPageException | InvalidSlotNumberException
				| DeleteRecException | HashEntryNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	private boolean delete(PageId currentPid, KeyDataEntry keyToDelete)
			throws KeyNotMatchException, NodeNotMatchException,
			ConvertException, IOException, ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			ConstructPageException, InvalidSlotNumberException,
			DeleteRecException, HashEntryNotFoundException {
		BTSortedPage curPage = new BTSortedPage(currentPid, headerPage.keyType);
		if (curPage.getType() == NodeType.INDEX) {

			RID firstRid = curPage.firstRecord();
			Tuple firstTuple = curPage.getRecord(firstRid);
			KeyDataEntry firstKeyDataEntry = BT.getEntryFromBytes(
					firstTuple.getTupleByteArray(), firstTuple.getOffset(),
					firstTuple.getLength(), headerPage.keyType, NodeType.INDEX);

			if (BT.keyCompare(keyToDelete.key, firstKeyDataEntry.key) < 0) {
				PageId forRecursionPageId = curPage.getPrevPage();
				SystemDefs.JavabaseBM.unpinPage(currentPid, false);
				return delete(forRecursionPageId, keyToDelete);

			} else {

				RID rid1 = new RID();
				RID rid2 = new RID();

				rid1 = curPage.firstRecord();

				Tuple tuple1 = new Tuple();
				Tuple tuple2 = new Tuple();
				PageId forRecursionPageId = new PageId();
				while (rid1 != null) {

					rid2 = curPage.nextRecord(rid1);

					if (rid2 == null) {
						tuple1 = curPage.getRecord(rid1);
						KeyDataEntry key1 = BT.getEntryFromBytes(
								tuple1.getTupleByteArray(), tuple1.getOffset(),
								tuple1.getLength(), headerPage.keyType,
								NodeType.INDEX);
						forRecursionPageId = (PageId) ((IndexData) (key1.data))
								.getData();
						SystemDefs.JavabaseBM.unpinPage(currentPid, false);
						return delete(forRecursionPageId, keyToDelete);

					} else {

						tuple2 = curPage.getRecord(rid2);
						KeyDataEntry key1 = BT.getEntryFromBytes(
								tuple1.getTupleByteArray(), tuple1.getOffset(),
								tuple1.getLength(), headerPage.keyType,
								NodeType.INDEX);

						KeyDataEntry key2 = BT.getEntryFromBytes(
								tuple2.getTupleByteArray(), tuple2.getOffset(),
								tuple2.getLength(), headerPage.keyType,
								NodeType.INDEX);

						if (BT.keyCompare(keyToDelete.key, key1.key) >= 0
								&& BT.keyCompare(keyToDelete.key, key2.key) < 0) {

							forRecursionPageId = (PageId) ((IndexData) (key1.data))
									.getData();
							break;

						} else {
							rid1 = curPage.nextRecord(rid1);
						}

					}

				}
				SystemDefs.JavabaseBM.unpinPage(currentPid, false);
				return delete(forRecursionPageId, keyToDelete);

			}

		} else {
			BTLeafPage btlp = (BTLeafPage) curPage;
			return btlp.delEntry(keyToDelete);

		}

	}

	public void goUp(Stack<Integer> path, KeyDataEntry copyUp)
			throws ConstructPageException, IOException, ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			InsertRecException, HashEntryNotFoundException, DiskMgrException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, InvalidBufferException,
			PageNotFoundException {
		/*
		 * base cases: * heya elroot * 3rfna n-insert mn 8er split pop mn
		 * elstack create PageId pin page 3la elid dh 7awel t-insert 3ady * lw
		 * nafa3 eshta w return * lw l2 * * split elpage eli enta 3ndha w 8yr
		 * elcopyUp w kamal meshwarak
		 */

		int pid = path.pop();
		PageId currentPid = new PageId(pid);
		BTIndexPage current = new BTIndexPage(currentPid, headerPage.keyType);
		RID indicator = current.insertRecord(copyUp);
		if (indicator == (null)) {
			// zebala
			BTIndexPage newPage = new BTIndexPage(headerPage.keyType);
			// elpage delw2ty pinned;
			BTIndexPage temp = new BTIndexPage(currentPid, headerPage.keyType);
			byte arr[] = new byte[current.getpage().length];
			for (int i = 0; i < arr.length; i++) {
				arr[i] = current.getpage()[i];
			}
			// SystemDefs.JavabaseBM.pinPage(currentPid, temp, true);
			emptyPage(current);
			for (int i = 0; i < arr.length; i++) {
				temp.getpage()[i] = arr[i];
			}
			// start a loop
			RID iterating = temp.firstRecord();
			boolean inserted = false;
			int initialSize = temp.pageSize();
			int size = 0;
			while (size < initialSize / 2) {
				Tuple tuple = temp.getRecord(iterating);
				KeyDataEntry kde = BT.getEntryFromBytes(
						tuple.getTupleByteArray(), tuple.getOffset(),
						tuple.getLength(), headerPage.keyType, NodeType.INDEX);
				if (!inserted) {
					if (BT.keyCompare(kde.key, copyUp.key) < 0 /*
																 * kde is
																 * smaller
																 */) {
						RID ins = current.insertRecord(kde);
						size += tuple.getLength();
						if (ins == null) {
							// throw exception
						}
						iterating.copyRid(temp.nextRecord(iterating));
					} else {
						inserted = true;
						// RID ins = current.insertRecord(copyUp);
						// size += getKeySize(copyUp);
						// if (ins == null) {
						// throw exception;
						// }
					}
				} else {
					RID ins = current.insertRecord(kde);
					size += tuple.getLength();
					iterating.copyRid(temp.nextRecord(iterating));
					if (ins == (null)) {
						// throw exception
					}
				}
			}// end of while
			boolean first = true;
			KeyDataEntry newCopyUp = null;
			while (iterating != (null)/* insert in new Page */) {
				Tuple tuple = temp.getRecord(iterating);
				KeyDataEntry kde = BT.getEntryFromBytes(
						tuple.getTupleByteArray(), tuple.getOffset(),
						tuple.getLength(), headerPage.keyType, NodeType.INDEX);
				if (!inserted) {
					if (BT.keyCompare(kde.key, copyUp.key) < 0 /*
																 * ked is
																 * smaller
																 */) {
						if (first) {
							first = false;
							newCopyUp = new KeyDataEntry(kde.key,
									newPage.getCurPage());
						} else {
							RID ins = newPage.insertRecord(kde);
							if (ins == null) {
								// throw exception
							}
							if (temp.nextRecord(iterating) != null)
								iterating.copyRid((temp.nextRecord(iterating)));
							else
								iterating = null;
						}
					}

					else {
						if (first) {
							first = false;
							newCopyUp = new KeyDataEntry(copyUp.key,
									newPage.getCurPage());
						} else {
							RID ins = newPage.insertRecord(kde);
							if (ins == null) {
								// throw exception
							}
							if (temp.nextRecord(iterating) != null)
								iterating.copyRid((temp.nextRecord(iterating)));
							else
								iterating = null;
						}
						inserted = true;
					}
				} else {
					if (first) {
						first = false;
						newCopyUp = new KeyDataEntry(kde.key,
								newPage.getCurPage());
					} else {
						RID ins = newPage.insertRecord(kde);
						if (ins == (null)) {
							// throw exception
						}
						if (temp.nextRecord(iterating) != null)
							iterating.copyRid((temp.nextRecord(iterating)));
						else
							iterating = null;
					}
				}
			}// end of while

			if (currentPid.pid == (headerPage.get_rootId().pid)) {
				BTIndexPage root = new BTIndexPage(headerPage.keyType);
				// enw root is pinned
				root.setLeftLink(currentPid);
				RID ins = root.insertRecord(newCopyUp);
				headerPage.setPointer(root.getCurPage());
				SystemDefs.JavabaseBM.unpinPage(currentPid, true);
				SystemDefs.JavabaseBM.unpinPage(currentPid, true);
				SystemDefs.JavabaseBM.unpinPage(newPage.getCurPage(), true);
				SystemDefs.JavabaseBM.unpinPage(root.getCurPage(), true);
				if (ins == null) {
					// throw exception;
				}
				return;
			}
			SystemDefs.JavabaseBM.unpinPage(currentPid, true);
			SystemDefs.JavabaseBM.unpinPage(currentPid, true);
			SystemDefs.JavabaseBM.unpinPage(newPage.getCurPage(), true);
			goUp(path, newCopyUp);
		} else {
			SystemDefs.JavabaseBM.unpinPage(currentPid, true);
			return;
		}
	}

	// private void goUp(Stack<Integer> stk, KeyDataEntry copyUp)
	// throws ConstructPageException, InsertRecException,
	// ReplacerException, PageUnpinnedException,
	// HashEntryNotFoundException, InvalidFrameNumberException,
	// InvalidSlotNumberException, IOException, HashOperationException,
	// PageNotReadException, BufferPoolExceededException,
	// PagePinnedException, BufMgrException, KeyNotMatchException,
	// NodeNotMatchException, ConvertException, DiskMgrException,
	// InvalidBufferException {
	// counter++;
	// System.out.println(counter + " counts");
	// int curPid = stk.pop();
	// PageId curPageId = new PageId(curPid);
	// BTIndexPage btsp = new BTIndexPage(curPageId, headerPage.keyType);
	//
	// RID indicatorRid = new RID();
	//
	// indicatorRid = btsp.insertRecord(copyUp);
	//
	// if (indicatorRid == null) {
	// BTIndexPage temPage = new BTIndexPage(curPageId, headerPage.keyType);
	// byte arr[] = new byte[btsp.getpage().length];
	// // byte arr[] = new byte[btsp.pa];
	//
	// for (int i = 0; i < btsp.pageSize(); i++) {
	// arr[i] = btsp.getpage()[i];
	// }
	//
	// // btsp.setpage(new byte[temPage.getpage().length]);
	//
	// emptyPage(btsp);
	// for (int i = 0; i < btsp.getpage().length; i++) {
	// temPage.getpage()[i] = arr[i];
	// }
	// // not sure
	// int compareSize = temPage.pageSize();
	// int size = 0;
	// RID iteratorRid = new RID();
	// iteratorRid.copyRid(temPage.firstRecord());
	// boolean copyUpInserted = false;
	//
	// while (size < compareSize / 2) {
	// Tuple tuple = new Tuple();
	// tuple = temPage.getRecord(iteratorRid);
	// KeyDataEntry kde = BT.getEntryFromBytes(
	// tuple.getTupleByteArray(), tuple.getOffset(),
	// tuple.getLength(), headerPage.keyType, NodeType.LEAF);
	//
	// if (BT.keyCompare(copyUp.key, kde.key) <= 0) {
	//
	// btsp.insertRecord(copyUp);
	// copyUpInserted = true;
	// size += (BT.getKeyLength(copyUp.key) + BT
	// .getDataLength(NodeType.INDEX));
	//
	// } else {
	//
	// btsp.insertRecord(kde);
	// size += tuple.getLength();
	// iteratorRid.copyRid(temPage.nextRecord(iteratorRid));
	//
	// }
	//
	// }
	//
	// BTIndexPage newPage = new BTIndexPage(headerPage.keyType);
	//
	// if (copyUpInserted) {
	//
	// Tuple ttt = new Tuple();
	// ttt = temPage.getRecord(iteratorRid);
	// KeyDataEntry nnn = BT.getEntryFromBytes(
	// ttt.getTupleByteArray(), ttt.getOffset(),
	// ttt.getLength(), headerPage.keyType, NodeType.INDEX);
	//
	// iteratorRid.copyRid(temPage.nextRecord(iteratorRid));
	//
	// while (iteratorRid != null) {
	//
	// Tuple tuple = new Tuple();
	// tuple = temPage.getRecord(iteratorRid);
	//
	// KeyDataEntry kde = BT.getEntryFromBytes(
	// tuple.getTupleByteArray(), tuple.getOffset(),
	// tuple.getLength(), headerPage.keyType,
	// NodeType.INDEX);
	// kde.data = copyUp.data;
	// newPage.insertRecord(kde);
	//
	// }
	// if (stk.size() == 0) {
	// BTIndexPage root = new BTIndexPage(headerPage.keyType);
	// root.insertRecord(nnn);
	// headerPage.setPointer(root.getCurPage());
	// root.setLeftLink(btsp.getCurPage());
	// SystemDefs.JavabaseBM.unpinPage(newPage.getCurPage(), true);
	// temPage.setpage(btsp.getpage());
	// SystemDefs.JavabaseBM.unpinPage(btsp.getCurPage(), true);
	// SystemDefs.JavabaseBM.unpinPage(btsp.getCurPage(), true);
	// SystemDefs.JavabaseBM.unpinPage(root.getCurPage(), true);
	// return;
	// }
	// SystemDefs.JavabaseBM.unpinPage(newPage.getCurPage(), true);
	// temPage.setpage(btsp.getpage());
	// SystemDefs.JavabaseBM.unpinPage(btsp.getCurPage(), true);
	// SystemDefs.JavabaseBM.unpinPage(btsp.getCurPage(), true);
	// goUp(stk, nnn);
	//
	// } else {
	//
	// RID copyIteratorRid = new RID();
	// copyIteratorRid.copyRid(iteratorRid);
	//
	// Tuple firstTuple = new Tuple();
	// firstTuple = temPage.getRecord(iteratorRid);
	// KeyDataEntry kde1 = BT.getEntryFromBytes(
	// firstTuple.getTupleByteArray(), firstTuple.getOffset(),
	// firstTuple.getLength(), headerPage.keyType,
	// NodeType.INDEX);
	// kde1.data = copyUp.data;
	// boolean checker = false;
	// if (BT.keyCompare(kde1.key, copyUp.key) < 0) {
	// checker = true;
	//
	// }
	//
	// if (checker) {
	//
	// KeyDataEntry newCopyUp = new KeyDataEntry(kde1.key,
	// kde1.data);
	//
	// iteratorRid.copyRid(temPage.nextRecord(iteratorRid));
	//
	// while (iteratorRid != null) {
	// Tuple tt = new Tuple();
	// tt = temPage.getRecord(iteratorRid);
	// KeyDataEntry nowKde = BT.getEntryFromBytes(
	// tt.getTupleByteArray(), tt.getOffset(),
	// tt.getLength(), headerPage.keyType,
	// NodeType.INDEX);
	// nowKde.data = copyUp.data;
	// newPage.insertRecord(nowKde);
	//
	// }
	// newPage.insertRecord(copyUp);
	// if (stk.size() == 0) {
	// BTIndexPage root = new BTIndexPage(headerPage.keyType);
	// root.insertRecord(newCopyUp);
	// headerPage.setPointer(root.getCurPage());
	// root.setLeftLink(btsp.getCurPage());
	// SystemDefs.JavabaseBM.unpinPage(newPage.getCurPage(),
	// true);
	// temPage.setpage(btsp.getpage());
	// SystemDefs.JavabaseBM
	// .unpinPage(btsp.getCurPage(), true);
	// SystemDefs.JavabaseBM
	// .unpinPage(btsp.getCurPage(), true);
	// SystemDefs.JavabaseBM
	// .unpinPage(root.getCurPage(), true);
	// return;
	// }
	// SystemDefs.JavabaseBM.unpinPage(newPage.getCurPage(), true);
	// temPage.setpage(btsp.getpage());
	// SystemDefs.JavabaseBM.unpinPage(btsp.getCurPage(), true);
	// SystemDefs.JavabaseBM.unpinPage(btsp.getCurPage(), true);
	// goUp(stk, newCopyUp);
	//
	// } else {
	//
	// boolean insertedagain = false;
	//
	// while (iteratorRid != null) {
	//
	// Tuple tuple = new Tuple();
	// tuple = temPage.getRecord(iteratorRid);
	// KeyDataEntry kde = BT.getEntryFromBytes(
	// tuple.getTupleByteArray(), tuple.getOffset(),
	// tuple.getLength(), headerPage.keyType,
	// NodeType.LEAF);
	// kde.data = copyUp.data;
	// newPage.insertRecord(kde);
	//
	// }
	// if (stk.size() == 0) {
	// BTIndexPage root = new BTIndexPage(headerPage.keyType);
	// root.insertRecord(copyUp);
	// root.setLeftLink(btsp.getCurPage());
	// SystemDefs.JavabaseBM.unpinPage(newPage.getCurPage(),
	// true);
	// headerPage.setPointer(root.getCurPage());
	// temPage.setpage(btsp.getpage());
	// SystemDefs.JavabaseBM
	// .unpinPage(btsp.getCurPage(), true);
	// SystemDefs.JavabaseBM
	// .unpinPage(btsp.getCurPage(), true);
	// SystemDefs.JavabaseBM
	// .unpinPage(root.getCurPage(), true);
	// return;
	// }
	// SystemDefs.JavabaseBM.unpinPage(newPage.getCurPage(), true);
	// temPage.setpage(btsp.getpage());
	// SystemDefs.JavabaseBM.unpinPage(btsp.getCurPage(), true);
	// SystemDefs.JavabaseBM.unpinPage(btsp.getCurPage(), true);
	// goUp(stk, copyUp);
	//
	// }
	//
	// }
	//
	// }
	//
	// else {
	//
	// SystemDefs.JavabaseBM.unpinPage(curPageId, true);
	// return;
	//
	// }
	//
	// }

	public Pair getPair(KeyClass key) throws ConstructPageException,
			ReplacerException, HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			IOException, InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, HashEntryNotFoundException {
		Pair toreturn = new Pair();
		BTSortedPage btsp2 = new BTSortedPage(headerPage.keyType);
		SystemDefs.JavabaseBM.pinPage(headerPage.get_rootId(), btsp2, true);
		boolean first = true;
		PageId yourpid = new PageId();
		yourpid.copyPageId(headerPage.get_rootId());
		PageId mypid = new PageId();
		mypid.copyPageId(headerPage.get_rootId());
		RID rid1, rid2;
		rid1 = btsp2.firstRecord();
		rid2 = btsp2.nextRecord(rid1);
		while (btsp2.getType() != NodeType.LEAF) {
			// fih pin
			yourpid.copyPageId(mypid);
			if (!first) {
				SystemDefs.JavabaseBM.pinPage(mypid, btsp2, true);
			} else
				first = true;
			while (!rid1.equals(null)) {
				Tuple t1, t2;
				t1 = new Tuple();
				t2 = new Tuple();
				t1 = btsp2.getRecord(rid1);
				t2 = btsp2.getRecord(rid2);
				KeyDataEntry k1, k2;
				k1 = BT.getEntryFromBytes(t1.getTupleByteArray(),
						t1.getOffset(), t1.getLength(), headerPage.keyType,
						NodeType.INDEX);
				k2 = BT.getEntryFromBytes(t2.getTupleByteArray(),
						t2.getOffset(), t2.getLength(), headerPage.keyType,
						NodeType.INDEX);

				if (k2.equals(null)) {
					// aro7 llpath bta3 k1
					if (rid1.equals(btsp2.firstRecord())) {
						if (BT.keyCompare(k1.key, key) < 0) {
							mypid.copyPageId(((BTIndexPage) btsp2)
									.getLeftLink());
							break;
						}
					}
					mypid.copyPageId((PageId) ((IndexData) k1.data).getData());
					break;
				} else if (BT.keyCompare(k1.key, key) >= 0
						&& BT.keyCompare(k2.key, key) < 0) {
					// hnro7 lPageID bta3et k1
					mypid.copyPageId((PageId) ((IndexData) k1.data).getData());
					continue;
				} else {
					rid1.copyRid(rid2);
					rid2 = btsp2.nextRecord(rid1);
					continue;
				}
			} // end of while
			SystemDefs.JavabaseBM.unpinPage(yourpid, false);
		} // end of while
			// kotka is leaf page containing the key
		PageId toreturnPid = btsp2.getCurPage();
		RID toreturnRID = btsp2.firstRecord();
		while (!toreturnRID.equals(null)) {
			Tuple t = btsp2.getRecord(toreturnRID);
			KeyDataEntry kde = BT.getEntryFromBytes(t.getTupleByteArray(),
					t.getOffset(), t.getLength(), headerPage.keyType,
					NodeType.LEAF);
			if (BT.keyCompare(key, kde.key) == 0)
				break;
		}
		if (toreturnRID.equals(null)) {
			// throw Exception
		}
		toreturn = new Pair(toreturnPid, toreturnRID);
		return toreturn;
	}

	public BTreeHeaderPage getHeaderPage() {
		// strange method msh 3arfin npinha 3la anhy pageId
		return this.headerPage;
	}

	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
			throws ConstructPageException, ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException,
			HashEntryNotFoundException, IOException {
		Pair one, two;
		one = getPair(lo_key);
		two = getPair(hi_key);
		BTFileScan toreturn = new BTFileScan(one, two, headerPage.keyType);
		return toreturn;
	}

	public void close() throws IOException {
		if (headerPage != null) {
			try {
				SystemDefs.JavabaseBM.unpinPage(headerPage.getCurPage(), true);
			} catch (ReplacerException | PageUnpinnedException
					| HashEntryNotFoundException | InvalidFrameNumberException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			headerPage = null;
		}
		open = false;
	}

	public void destroyFile() throws InvalidSlotNumberException,
			KeyNotMatchException, NodeNotMatchException, ConvertException,
			ConstructPageException, IOException {
		open = false;
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != -1)
				completeDestroy(pgId);
			try {
				SystemDefs.JavabaseBM.unpinPage(headerPage.getCurPage(), false);
				SystemDefs.JavabaseBM.freePage(headerPage.getCurPage());
				SystemDefs.JavabaseDB.delete_file_entry(dbname);
			} catch (ReplacerException | PageUnpinnedException
					| HashEntryNotFoundException | InvalidFrameNumberException
					| InvalidBufferException | HashOperationException
					| PageNotReadException | BufferPoolExceededException
					| PagePinnedException | BufMgrException | DiskMgrException
					| IOException | FileEntryNotFoundException
					| FileIOException | InvalidPageNumberException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			headerPage = null;
		}
	}

	public void completeDestroy(PageId pageno)
			throws InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, ConstructPageException {

		BTSortedPage sortedPage;
		Page page = new Page();
		try {
			SystemDefs.JavabaseBM.pinPage(pageno, page, false);
			sortedPage = new BTSortedPage(page, headerPage.get_keyType());

			if (sortedPage.getType() == NodeType.INDEX) {
				BTIndexPage indexPage = new BTIndexPage(page,
						headerPage.get_keyType());
				RID rid = new RID();
				PageId childId;
				KeyDataEntry entry;
				for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage
						.getNext(rid)) {
					childId = ((IndexData) (entry.data)).getData();
					completeDestroy(childId);
					SystemDefs.JavabaseBM.unpinPage(pageno, false);
					SystemDefs.JavabaseBM.freePage(pageno);
				}
			} else {
				SystemDefs.JavabaseBM.unpinPage(pageno, false);
				SystemDefs.JavabaseBM.freePage(pageno);
			}
		} catch (ReplacerException | PageUnpinnedException
				| HashEntryNotFoundException | InvalidFrameNumberException
				| InvalidBufferException | HashOperationException
				| PageNotReadException | BufferPoolExceededException
				| PagePinnedException | BufMgrException | DiskMgrException
				| IOException e) {
			e.printStackTrace();
		}

	}

	public void traceFilename(String str) {
	}
}
