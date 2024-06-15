package simpledb.tx

import io.kotest.core.spec.style.FunSpec
import simpledb.tx.recovery.Transaction
import simpledb.server.SimpleDB

class TxTest: FunSpec({
    fun `test transaction`() {
        val db = SimpleDB("testdb")
        val fileMgr = db.fileMgr
        val logMgr = db.logMgr
        val bufferMgr = db.bufferMgr

        val tx = Transaction(fileMgr, logMgr, bufferMgr)

    }
})