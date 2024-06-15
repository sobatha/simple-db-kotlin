package simpledb.tx.recovery

import simpledb.file.BlockId
import simpledb.file.FileMgr
import simpledb.buffer.BufferMgr
import simpledb.log.LogMgr

class Transaction(val fileMgr: FileMgr, val logMgr: LogMgr, val bufferMgr: BufferMgr) {
    fun pin(blk: BlockId) {
        TODO()
    }

    fun setString(blk: BlockId, offset: Int, log_value: Any, is_need_logged: Boolean) {
        TODO()
    }

    fun unpin(blk: BlockId) {
        TODO()
    }

    fun setInt(blk: BlockId, offset: Int, intVal: Int, b: Boolean) {
        TODO()
    }

}
