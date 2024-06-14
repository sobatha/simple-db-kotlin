package org.example.simpledb.tx.recovery

import org.example.simpledb.file.Page

class SetIntRecord(p: Page) : LogRecord {
    override fun writeTo(w: Page) {
        TODO("Not yet implemented")
    }

    override val op: Int
        get() = TODO("Not yet implemented")
    override val txNumber: Int
        get() = TODO("Not yet implemented")

    override fun undo(txnum: Int) {
        TODO("Not yet implemented")
    }

}
