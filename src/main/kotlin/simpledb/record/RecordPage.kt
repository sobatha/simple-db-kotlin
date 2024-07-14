package simpledb.record

import simpledb.file.BlockId
import simpledb.tx.recovery.Transaction

class RecordPage(
    private val transaction: Transaction,
    val blockId: BlockId,
    private val layout: Layout
) {
    init { transaction.pin(blockId) }

    fun getInt(slot: Int, fieldName: String) =
        transaction.getInt(blockId, offset(slot) + layout.offset(fieldName)!!)

    fun getString(slot: Int, fieldName: String) =
        transaction.getString(blockId, offset(slot) + layout.offset(fieldName)!!)

    fun setInt(slot: Int, fieldName: String, value: Int) =
        transaction.setInt(blockId, offset(slot) + layout.offset(fieldName)!!, value)

    fun setString(slot: Int, fieldName: String, value: String) =
        transaction.setString(blockId, offset(slot) + layout.offset(fieldName)!!, value)

    fun delete(slot: Int) = setFlag(slot, DeleteFlag.EMPTY)

    fun format() {
        var slot = 0
        while (isValidSlot(slot)) {
            transaction.setInt(blockId, offset(slot), DeleteFlag.EMPTY.value, false)
            val schema = layout.schema
            schema.fields.forEach {name ->
                val fieldPosition = offset(slot) + layout.offset(name)!!
                when (schema.type(name)) {
                    FieldType.VARCHAR -> transaction.setString(blockId, fieldPosition, "", false)
                    FieldType.INTEGER -> transaction.setInt(blockId, fieldPosition, 0, false)
                }
            }
            slot++
        }
    }

    fun nextAfter(slot: Int) = searchAfter(slot, DeleteFlag.USED)

    fun insertAfter(slot: Int): Int {
        val newSlot = searchAfter(slot, DeleteFlag.EMPTY)
        if (newSlot >= 0) setFlag(newSlot, DeleteFlag.USED)
        return newSlot
    }

    private fun searchAfter(slot: Int, flag: RecordPage.DeleteFlag): Int {
        var localSlot = slot + 1
        while (isValidSlot(localSlot)) {
            if (transaction.getInt(blockId, offset(localSlot)) == flag.value)  return localSlot
            localSlot++
        }
        return -1
    }

    fun isValidSlot(slot: Int) = offset(slot+1) <= transaction.blkSize()

    fun setFlag(slot: Int, flag: DeleteFlag) = transaction.setInt(blockId, offset(slot), flag.value, true)
    fun offset(slot: Int) = slot * layout.slotSize
    enum class DeleteFlag(val value: Int) { EMPTY(0), USED(-1) }
}