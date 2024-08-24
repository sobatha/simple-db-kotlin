package simpledb.tx.recovery

import simpledb.file.Page
import simpledb.log.LogMgr
import simpledb.tx.Transaction

class StartRecord(page: Page) : LogRecord {
    val txNum = page.getInt(Integer.BYTES)

    override val op: LogType
        get() = LogType.START

    override val txNumber: Int
        get() = txNum

    override fun undo(tx: Transaction) {}

    companion object {
        fun writeToLog(logMgr: LogMgr, txNum: Int) = LogRecord.writeToLog(logMgr, txNum, LogType.START)
    }

}
