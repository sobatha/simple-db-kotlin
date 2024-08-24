package simpledb.metadata

import simpledb.tx.Transaction
import simpledb.record.*

class MetadataMgr(isNew: Boolean, tx: Transaction) {
    private val tblmgr: TableMgr = TableMgr(isNew, tx)
    private val viewmgr: ViewMgr = ViewMgr(isNew, tblmgr, tx)
    private val statmgr: StatMgr = StatMgr(tblmgr, tx)
    private val idxmgr: IndexMgr = IndexMgr(isNew, tblmgr, statmgr, tx)

    fun createTable(tblname: String, sch: Schema, tx: Transaction) {
        tblmgr.createTable(tblname, sch, tx)
    }

    fun getLayout(tblname: String, tx: Transaction): Layout {
        return tblmgr.getLayout(tblname, tx)
    }

    fun createView(viewname: String, viewdef: String, tx: Transaction) {
        viewmgr.createView(viewname, viewdef, tx)
    }

    fun getViewDef(viewname: String, tx: Transaction): String? {
        return viewmgr.getViewDef(viewname, tx)
    }

    fun createIndex(idxname: String, tblname: String, fldname: String, tx: Transaction) {
        idxmgr.createIndex(idxname, tblname, fldname, tx)
    }

    fun getIndexInfo(tblname: String, tx: Transaction): Map<String, IndexInfo> {
        return idxmgr.getIndexInfo(tblname, tx)
    }

    fun getStatInfo(tblname: String, layout: Layout, tx: Transaction): StatInfo {
        return statmgr.getStatInfo(tblname, layout, tx)
    }
}
