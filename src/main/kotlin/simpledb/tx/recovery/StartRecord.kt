package org.example.simpledb.tx.recovery

import org.example.simpledb.file.Page

class StartRecord(page: Page) : LogRecord {
    val txNum = page.getInt(Integer.BYTES)

    override val op: LogType
        get() = LogType.START

    override val txNumber: Int
        get() = txNum

    override fun undo(tx: Transaction) {}

}
