package simpledb.tx

import io.kotest.core.spec.style.FunSpec
import simpledb.file.BlockId
import simpledb.server.SimpleDB
import simpledb.tx.recovery.RecoveryMgr
import simpledb.tx.recovery.SetStringRecord
import simpledb.tx.recovery.Transaction


fun main() {
    val db = SimpleDB("recoverytestdb")
    val fileMgr = db.fileMgr
    val logMgr = db.logMgr
    val bufferMgr = db.bufferMgr

    val tx = Transaction(fileMgr, logMgr, bufferMgr)
    val blk = BlockId("testlog", 1)
    tx.pin(blk)
    val buff = bufferMgr.getBuffer(blk)!!
//    tx.recoveryMgr.setString(buff, 0,"hello")
    SetStringRecord.writeToLog(logMgr, 0, blk, 0, "hello")
}

