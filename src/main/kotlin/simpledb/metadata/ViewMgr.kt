package simpledb.metadata

import simpledb.record.*
import simpledb.tx.Transaction

class ViewMgr(isNew: Boolean, private val tblMgr: TableMgr, tx: Transaction) {
    companion object {
        // the max chars in a view definition.
        private const val MAX_VIEWDEF = 100
    }

    init {
        if (isNew) {
            val sch = Schema().apply {
                addStringField("viewname", TableMgr.MAX_NAME)
                addStringField("viewdef", MAX_VIEWDEF)
            }
            tblMgr.createTable("viewcat", sch, tx)
        }
    }

    fun createView(vname: String, vdef: String, tx: Transaction) {
        val layout = tblMgr.getLayout("viewcat", tx)
        TableScan(tx, "viewcat", layout).let { ts ->
            ts.insert()
            ts.setString("viewname", vname)
            ts.setString("viewdef", vdef)
            ts.close()
        }
    }

    fun getViewDef(vname: String, tx: Transaction): String? {
        var result: String? = null
        val layout = tblMgr.getLayout("viewcat", tx)
        TableScan(tx, "viewcat", layout).let { ts ->
            while (ts.next()) {
                if (ts.getString("viewname") == vname) {
                    result = ts.getString("viewdef")
                }
            }
            ts.close()
        }
        return result
    }
}