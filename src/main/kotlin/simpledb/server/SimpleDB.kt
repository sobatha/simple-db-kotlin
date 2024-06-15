package simpledb.server

import simpledb.file.FileMgr
import simpledb.log.LogMgr


class SimpleDB(val dirName: String) {
    companion object {
        const val BUFFER_SIZE = 8
        const val LOG_FILE = "simpledb.log"
    }

    val fileMgr = FileMgr(dirName, BUFFER_SIZE)
    val logMgr = LogMgr(fileMgr, LOG_FILE)
    val bufferMgr = simpledb.buffer.BufferMgr(fileMgr, logMgr, BUFFER_SIZE)


}