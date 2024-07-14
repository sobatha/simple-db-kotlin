package simpledb.server

import simpledb.file.FileMgr
import simpledb.log.LogMgr
import simpledb.tx.recovery.Transaction


class SimpleDB(val dirName: String) {
    fun newTx(): Transaction {
        return Transaction(fileMgr, logMgr, bufferMgr)
    }

    companion object {
        const val BUFFER_SIZE = 8
        const val BLOCK_SIZE = 400
        const val LOG_FILE = "logfile"
    }

    val fileMgr = FileMgr(dirName, BLOCK_SIZE)
    val logMgr = LogMgr(fileMgr, LOG_FILE)
    val bufferMgr = simpledb.buffer.BufferMgr(fileMgr, logMgr, BUFFER_SIZE)


}