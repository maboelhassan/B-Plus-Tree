package btree;

import global.PageId;
import global.RID;
public class Pair {
	PageId pairPageId;
	RID paiRid;

	Pair() {
		pairPageId = null;
		paiRid = null;
	}

	Pair(PageId pid, RID rid) {
		pairPageId.copyPageId(pid);
		paiRid.copyRid(rid);
	}
}
