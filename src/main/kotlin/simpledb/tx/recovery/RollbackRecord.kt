package simpledb.tx.recovery

import simpledb.file.Page

class RollbackRecord(page: Page) : LogRecord {
    val txNum = page.getInt(Integer.BYTES)

    override val op: LogType
        get() = LogType.ROLLBACK

    override val txNumber: Int
        get() = txNum

    override fun undo(tx: Transaction) {}

}
