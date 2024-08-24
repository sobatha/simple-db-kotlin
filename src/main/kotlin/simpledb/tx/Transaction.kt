package simpledb.tx

import simpledb.tx.concurrency.ConcurrencyMgr
import simpledb.buffer.Buffer
import simpledb.file.BlockId
import simpledb.file.FileMgr
import simpledb.buffer.BufferMgr
import simpledb.log.LogMgr
import simpledb.tx.recovery.RecoveryMgr

class Transaction(val fileMgr: FileMgr, val logMgr: LogMgr, val bufferMgr: BufferMgr) {
    companion object {
        private val END_OF_FILE = -1
        private var nextTxNumber = 0
    }
    private val txNum = getNextTxNumber()
    private val recoveryMgr = RecoveryMgr(this, txNum, logMgr, bufferMgr)
    private val concurrencyMgr = ConcurrencyMgr()
    private val myBuffers = mutableMapOf<BlockId, Buffer>()

    @Synchronized
    private fun getNextTxNumber(): Int {
        nextTxNumber++
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

    fun setString(blk: BlockId, offset: Int, log_value: String, is_need_logged: Boolean = true) {
        concurrencyMgr.xLock(blk)
        val buff = bufferMgr.getBuffer(blk)!!
        val page = buff.contents
        var lsn = -1
        if (is_need_logged) {
            lsn = recoveryMgr.setString(buff, offset, log_value)
        }
        page.setString(offset, log_value)
        buff.setModified(txNum, lsn)
    }

    fun setInt(blk: BlockId, offset: Int, log_value: Int, is_need_logged: Boolean = true) {
        concurrencyMgr.xLock(blk)
        val buff = bufferMgr.getBuffer(blk)!!
        var lsn = -1
        if (is_need_logged) {
            lsn = recoveryMgr.setInt(buff, offset, log_value)
        }
        val page = buff.contents
        page.setInt(offset, log_value)
        buff.setModified(txNum, lsn)
    }


    fun commit() {
        recoveryMgr.commit()
        concurrencyMgr.release()
        bufferMgr.unpinAll()
    }

    fun rollback() {
        recoveryMgr.rollback()
        concurrencyMgr.release()
        bufferMgr.unpinAll()
    }
    fun getInt(blk: BlockId, i: Int): Int? {
        concurrencyMgr.sLock(blk)
        return bufferMgr.getBuffer(blk)?.contents?.getInt(i)
    }
    fun getString(blk: BlockId, i: Int): String? {
        concurrencyMgr.sLock(blk)
        return bufferMgr.getBuffer(blk)?.contents?.getString(i)
    }

    fun size(fileName: String): Int {
        val dummyBlk = BlockId(fileName, END_OF_FILE)
        concurrencyMgr.sLock(dummyBlk)
        return fileMgr.length(fileName)
    }

    fun append(fileName: String): BlockId {
        val dummyBlk = BlockId(fileName, END_OF_FILE)
        concurrencyMgr.xLock(dummyBlk)
        return fileMgr.append(fileName)
    }

    fun blkSize() = fileMgr.blockSize

    fun availableBuffers() = bufferMgr.numAvailable

    fun recover() {
        bufferMgr.flushAll(txNum)
        recoveryMgr.recover()
    }
}
