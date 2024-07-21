package simpledb.metadata

import simpledb.record.*
import simpledb.tx.recovery.Transaction

class IndexMgr(isNew: Boolean, private val tblmgr: TableMgr, private val statmgr: StatMgr, tx: Transaction) {
    private val layout: Layout

    init {
        if (isNew) {
            val sch = Schema().apply {
                addStringField("indexname", TableMgr.MAX_NAME)
                addStringField("tablename", TableMgr.MAX_NAME)
                addStringField("fieldname", TableMgr.MAX_NAME)
            }
            tblmgr.createTable("idxcat", sch, tx)
        }
        layout = tblmgr.getLayout("idxcat", tx)
    }

    /**
     * Create an index of the specified type for the specified field.
     * A unique ID is assigned to this index, and its information
     * is stored in the idxcat table.
     * @param idxname the name of the index
     * @param tblname the name of the indexed table
     * @param fldname the name of the indexed field
     * @param tx the calling transaction
     */
    fun createIndex(idxname: String, tblname: String, fldname: String, tx: Transaction) {
        TableScan(tx, "idxcat", layout).let { ts ->
            ts.insert()
            ts.setString("indexname", idxname)
            ts.setString("tablename", tblname)
            ts.setString("fieldname", fldname)
            ts.close()
        }
    }

    /**
     * Return a map containing the index info for all indexes
     * on the specified table.
     * @param tblname the name of the table
     * @param tx the calling transaction
     * @return a map of IndexInfo objects, keyed by their field names
     */
    fun getIndexInfo(tblname: String, tx: Transaction): Map<String, IndexInfo> {
        val result = mutableMapOf<String, IndexInfo>()
        TableScan(tx, "idxcat", layout).let { ts ->
            while (ts.next()) {
                if (ts.getString("tablename") == tblname) {
                    val idxname = ts.getString("indexname")
                    val fldname = ts.getString("fieldname")
                    val tblLayout = tblmgr.getLayout(tblname, tx)
                    val tblsi = statmgr.getStatInfo(tblname, tblLayout, tx)
                    val ii = IndexInfo(idxname, fldname, tblLayout.schema, tx, tblsi)
                    result[fldname] = ii
                }
            }
            ts.close()
        }
        return result
    }
}
