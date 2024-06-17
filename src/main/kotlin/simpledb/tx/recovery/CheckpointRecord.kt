package simpledb.tx.recovery

import simpledb.file.BlockId
import simpledb.file.Page
import simpledb.log.LogMgr

class CheckpointRecord(page: Page) : LogRecord {
    val txNum = page.getInt(Integer.BYTES)

    override val op: LogType
        get() = LogType.CHECKPOINT

    override val txNumber: Int
        get() = txNum

    override fun undo(tx: Transaction) {}

    companion object {
        fun writeToLog(logMgr: LogMgr, txNum: Int) = LogRecord.writeToLog(logMgr, txNum, LogType.CHECKPOINT)
    }
}
