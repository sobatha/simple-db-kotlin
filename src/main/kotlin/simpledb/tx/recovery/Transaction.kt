package simpledb.tx.recovery

import simpledb.buffer.Buffer
import simpledb.file.BlockId
import simpledb.file.FileMgr
import simpledb.buffer.BufferMgr
import simpledb.file.Page
import simpledb.log.LogMgr

class Transaction(val fileMgr: FileMgr, val logMgr: LogMgr, val bufferMgr: BufferMgr) {
    companion object {
        private var nextTxNumber = 0
    }
    private val txNum = getNextTxNumber()
    val recoveryMgr = RecoveryMgr(this, txNum, logMgr, bufferMgr)
    private val myBuffers = mutableMapOf<BlockId, Buffer>()

    @Synchronized
    private fun getNextTxNumber(): Int {
        nextTxNumber++
        println("nextTxNumber: $nextTxNumber")
        return nextTxNumber
    }

    fun pin(blk: BlockId) {
        val buff = bufferMgr.pin(blk)
        myBuffers[blk] = buff
    }

    fun unpin(blk: BlockId) {
        bufferMgr.unpin(myBuffers[blk]!!)
        myBuffers.remove(blk)
    }

    fun setString(blk: BlockId, offset: Int, log_value: String, is_need_logged: Boolean) {
        val buff = bufferMgr.getBuffer(blk)!!
        val page = buff.contents
        var lsn = -1
        if (is_need_logged) {
            lsn = recoveryMgr.setString(buff, offset, log_value)
        }
        page.setString(offset, log_value)
        buff.setModified(txNum, lsn)
//        TODO("implement concurrency control")
    }

    fun setInt(blk: BlockId, offset: Int, log_value: Int, is_need_logged: Boolean) {
        val buff = bufferMgr.getBuffer(blk)!!
        val page = buff.contents
        var lsn = -1
        if (is_need_logged) {
            lsn = recoveryMgr.setInt(buff, offset, log_value)
        }
        page.setInt(offset, log_value)
        buff.setModified(txNum, lsn)
//        TODO("implement concurrency control")
    }


    fun commit() {
        recoveryMgr.commit()
        bufferMgr.unpinAll()
//        TODO("implement concurrency control")
    }

    fun rollback() {
        recoveryMgr.rollback()
        bufferMgr.unpinAll()
//        TODO("implement concurrency control")
    }

//        TODO("implement concurrency control")
    fun getInt(blk: BlockId, i: Int): Int? = bufferMgr.getBuffer(blk)?.contents?.getInt(i)
//        TODO("implement concurrency control")
    fun getString(blk: BlockId, i: Int) = bufferMgr.getBuffer(blk)?.contents?.getString(i)


}
