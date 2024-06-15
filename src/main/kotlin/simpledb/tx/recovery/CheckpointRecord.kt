package simpledb.tx.recovery

import simpledb.file.BlockId
import simpledb.file.Page

class CheckpointRecord(page: Page) : LogRecord {
    val txNum = page.getInt(Integer.BYTES)

    override val op: LogType
        get() = LogType.CHECKPOINT

    override val txNumber: Int
        get() = txNum

    override fun undo(tx: Transaction) {}
}
