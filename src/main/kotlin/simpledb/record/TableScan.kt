package simpledb.record

import simpledb.query.Constant
import simpledb.file.BlockId
import simpledb.query.UpdateScan
import simpledb.tx.recovery.Transaction

class TableScan(
    private val transaction: Transaction,
    tableName: String,
    private val layout: Layout
) : UpdateScan {
    private var recordPage: RecordPage? = null
    private val filename: String = "$tableName.tbl"
    private var currentslot: Int = -1

    init {
        if (transaction.size(filename) == 0) {
            moveToNewBlock()
        } else {
            moveToBlock(0)
        }
    }

    // Methods that implement Scan

    override fun beforeFirst() {
        moveToBlock(0)
    }

    override fun next(): Boolean {
        currentslot = recordPage!!.nextAfter(currentslot)
        while (currentslot < 0) {
            if (atLastBlock()) return false
            moveToBlock(recordPage!!.blockId.number + 1)
            currentslot = recordPage!!.nextAfter(currentslot)
        }
        return true
    }

    override fun getInt(fldname: String): Int {
        return recordPage!!.getInt(currentslot, fldname)!!
    }

    override fun getString(fldname: String): String {
        return recordPage!!.getString(currentslot, fldname)!!
    }

    override fun getVal(fldname: String): Constant {
        return when (layout.schema.type(fldname)) {
         FieldType.INTEGER -> Constant(getInt(fldname))
         FieldType.VARCHAR -> Constant(getString(fldname))
        }
    }

    override fun hasField(fldname: String): Boolean {
        return layout.schema.hasField(fldname)
    }

    override fun close() {
        if (recordPage != null) transaction.unpin(recordPage!!.blockId)
    }

    // Methods that implement UpdateScan

    override fun setInt(fldname: String, value: Int) {
        recordPage!!.setInt(currentslot, fldname, value)
    }

    override fun setString(fldname: String, value: String) {
        recordPage!!.setString(currentslot, fldname, value)
    }

    override fun setVal(fldname: String, value: Constant) {
        when (layout.schema.type(fldname)) {
            FieldType.INTEGER -> setInt(fldname, value.asInt()!!)
            FieldType.VARCHAR -> setString(fldname, value.asString()!!)
        }
    }

    override fun insert() {
        currentslot = recordPage!!.insertAfter(currentslot)
        while (currentslot < 0) {
            if (atLastBlock()) {
                moveToNewBlock()
            } else {
                moveToBlock(recordPage!!.blockId.number + 1)
            }
            currentslot = recordPage!!.insertAfter(currentslot)
        }
    }

    override fun delete() {
        recordPage!!.delete(currentslot)
    }

    override fun moveToRid(rid: RID) {
        close()
        val blk = BlockId(filename, rid.blockNumber)
        recordPage = RecordPage(transaction, blk, layout)
        currentslot = rid.slot
    }

    override fun getRid(): RID {
        return RID(recordPage!!.blockId.number, currentslot)
    }

    // Private auxiliary methods

    private fun moveToBlock(blknum: Int) {
        close()
        val blk = BlockId(filename, blknum)
        recordPage = RecordPage(transaction, blk, layout)
        currentslot = -1
    }

    private fun moveToNewBlock() {
        close()
        val blk = transaction.append(filename)
        recordPage = RecordPage(transaction, blk, layout)
        recordPage!!.format()
        currentslot = -1
    }

    private fun atLastBlock(): Boolean {
        return recordPage!!.blockId.number == transaction.size(filename) - 1
    }
}
