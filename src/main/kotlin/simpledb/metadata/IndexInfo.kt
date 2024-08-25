package simpledb.metadata

import simpledb.tx.Transaction
import simpledb.record.*
import simpledb.index.hash.HashIndex

class IndexInfo(
    private val idxname: String,
    private val fldname: String,
    private val tblSchema: Schema,
    private val tx: Transaction,
    private val si: StatInfo
) {
    private val idxLayout: Layout = createIndexLayout()

    fun open(): HashIndex {
        return HashIndex(tx, idxname, idxLayout)
        // return BTreeIndex(tx, idxname, idxLayout)
    }

    fun blocksAccessed(): Int {
        val rpb = tx.blockSize() / idxLayout.slotSize()
        val numblocks = si.recordsOutput()
        return HashIndex.searchCost(numblocks, rpb)
        // return BTreeIndex.searchCost(numblocks, rpb)
    }

    fun recordsOutput(): Int {
        return si.recordsOutput() / si.distinctValues(fldname)
    }

    fun distinctValues(fname: String): Int {
        return if (fldname == fname) 1 else si.distinctValues(fldname)
    }

    private fun createIndexLayout(): Layout {
        val schema = Schema()
        schema.addIntField("block")
        schema.addIntField("id")
        if (schema.type(fldname).number == java.sql.Types.INTEGER) {
            schema.addIntField("dataval")
        } else {
            val fieldLength = schema.length(fldname) ?: throw RuntimeException("field length null error")
            schema.addStringField("datavale", fieldLength)
        }
        return Layout(schema)
    }
}
