package simpledb.tx

import io.kotest.core.spec.style.FunSpec
import simpledb.file.BlockId
import simpledb.tx.recovery.Transaction
import simpledb.server.SimpleDB

fun main() {
    val db = SimpleDB("testdb")
    val fileMgr = db.fileMgr
    val logMgr = db.logMgr
    val bufferMgr = db.bufferMgr

    val tx1 = Transaction(fileMgr, logMgr, bufferMgr)
    val blk = BlockId("filename", 1)
    tx1.pin(blk)

    // don't log initial block contents
    tx1.setInt(blk, 80, 1, false)
    tx1.setString(blk, 40, "one", false)
    tx1.commit()

    val tx2 = Transaction(fileMgr, logMgr, bufferMgr)
    tx2.pin(blk)
    println("reading values: ${tx2.getInt(blk, 80)} ${tx2.getString(blk, 40)}")
    tx2.setInt(blk, 80, 2, true)
    tx2.setString(blk, 40, "two", true)
    tx2.commit()

    val tx3 = Transaction(fileMgr, logMgr, bufferMgr)
    tx3.pin(blk)
    println("reading values: ${tx3.getInt(blk, 80)} ${tx3.getString(blk, 40)}")
    tx3.setInt(blk, 80, 99, true)
    tx3.setString(blk, 40, "nine", true)
    println("reading values: ${tx3.getInt(blk, 80)} ${tx3.getString(blk, 40)}")
    tx3.rollback()

    val tx4 = Transaction(fileMgr, logMgr, bufferMgr)
    tx4.pin(blk)
    println("reading values: ${tx4.getInt(blk, 80)} ${tx4.getString(blk, 40)}")
}
