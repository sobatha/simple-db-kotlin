package simpledb.metadata

import simpledb.tx.recovery.Transaction
import simpledb.record.*
import org.example.simpledb.index.hash.HashIndex

class IndexInfo(
    private val idxname: String,
    private val fldname: String,
    private val tblSchema: Schema,
    private val tx: Transaction,
    private val si: StatInfo
) {
    private val idxLayout: Layout = createIdxLayout()

    fun open(): HashIndex {
        return HashIndex(tx, idxname, idxLayout)
        // return BTreeIndex(tx, idxname, idxLayout)
    }

    fun blocksAccessed(): Int {
        val rpb = tx.blkSize() / idxLayout.slotSize
        val numblocks = si.recordsOutput().div(rpb)
        return HashIndex.searchCost(numblocks, rpb)
        // return BTreeIndex.searchCost(numblocks, rpb)
    }

    fun recordsOutput(): Int {
        return si.recordsOutput() / si.distinctValues(fldname)
    }

    fun distinctValues(fname: String): Int {
        return if (fldname == fname) 1 else si.distinctValues(fldname)
    }

    private fun createIdxLayout(): Layout {
        val sch = Schema()
        sch.addIntField("block")
        sch.addIntField("id")
        if (tblSchema.type(fldname) == FieldType.INTEGER)
            sch.addIntField("dataval")
        else {
            val fldlen = tblSchema.length(fldname)
            sch.addStringField("dataval", fldlen)
        }
        return Layout(sch)
    }
}
