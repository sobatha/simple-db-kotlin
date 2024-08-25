package simpledb.tx

import simpledb.buffer.Buffer
import simpledb.buffer.BufferMgr
import simpledb.file.BlockId
import simpledb.file.FileMgr
import simpledb.log.LogMgr
import simpledb.tx.concurrency.ConcurrencyMgr
import simpledb.tx.recovery.RecoveryMgr

class Transaction(
    val fileManager: FileMgr,
    val bufferManager: BufferMgr,
    val logManager: LogMgr,
) {
    private val enfOfFile = -1
    private var recoveryManager: RecoveryMgr
    private var concurrencyManager: ConcurrencyMgr
    private var transactionNumber: Int = nextTransactionNumber()
    private var myBuffers: BufferList

    init {
        recoveryManager = RecoveryMgr(this, transactionNumber, logManager, bufferManager)
        concurrencyManager = ConcurrencyMgr()
        myBuffers = BufferList(bufferManager)
    }

    fun commit() {
        recoveryManager.commit()
        concurrencyManager.release()
        myBuffers.unpinAll()
//        println("transaction $transactionNumber committed")
    }

    fun rollback() {
        recoveryManager.rollback()
        concurrencyManager.release()
        myBuffers.unpinAll()
//        println("transaction $transactionNumber rolled back")
    }

    fun  recover() {
        bufferManager.flushAll(transactionNumber)
        recoveryManager.recover()
    }

    fun pin(blockId: BlockId) {
        myBuffers.pin(blockId)
    }

    fun unpin(blockId: BlockId) {
        myBuffers.unpin(blockId)
    }

    fun getInt(blockId: BlockId, offset: Int): Int? {
        concurrencyManager.sLock(blockId)
        val buffer = myBuffers.getBuffer(blockId) ?: return null
        return buffer.contents().getInt(offset)
    }

    fun getString(blockId: BlockId, offset: Int): String? {
        concurrencyManager.sLock(blockId)
        val buffer = myBuffers.getBuffer(blockId) ?: return null
        return buffer.contents().getString(offset)
    }

    fun setInt(blockId: BlockId, offset: Int, value: Int, okToLog: Boolean) {
        concurrencyManager.xLock(blockId)
        val buffer = myBuffers.getBuffer(blockId)
        var lsn = -1
        if (buffer == null) return
        if (okToLog) lsn = recoveryManager.setInt(buffer, offset, value)
        val page = buffer.contents()
        page.setInt(offset, value)
        buffer.setModified(transactionNumber, lsn)
    }

    fun setString(blockId: BlockId, offset: Int, value: String, okToLog: Boolean) {
        concurrencyManager.xLock(blockId)
        val buffer = myBuffers.getBuffer(blockId)
        var lsn = -1
        if (buffer == null) return
        if (okToLog) lsn = recoveryManager.setString(buffer, offset, value)
        val page = buffer.contents()
        page.setString(offset, value)
        buffer.setModified(transactionNumber, lsn)
    }

    fun size(filename: String): Int {
        val dummyBlock = BlockId(filename, enfOfFile)
        concurrencyManager.sLock(dummyBlock)
        return fileManager.length(filename)
    }

    fun append(filename: String): BlockId {
        val dummyBlock = BlockId(filename, enfOfFile)
        concurrencyManager.xLock(dummyBlock)
        return fileManager.append(filename)
    }

    fun blockSize(): Int {
        return fileManager.blockSize
    }

    fun availableBuffers(): Int {
        return bufferManager.available()
    }

    companion object {
        var nextTransactionNumber = 0

        @Synchronized
        fun nextTransactionNumber(): Int {
            this.nextTransactionNumber++
//            println("new transaction $nextTransactionNumber")
            return nextTransactionNumber
        }
    }
}

class BufferList(private val bufferManager: BufferMgr) {
    private val buffers = mutableMapOf<BlockId, Buffer>()
    private val pins = mutableListOf<BlockId>()

    fun getBuffer(blockId: BlockId): Buffer? {
        return buffers[blockId]
    }

    fun pin(blockId: BlockId) {
        val buffer = bufferManager.pin(blockId)
        buffers[blockId] = buffer
        pins.add(blockId)
    }

    fun unpin(blockId: BlockId) {
        val buffer = buffers[blockId]
        if (buffer != null) {
            bufferManager.unpin(buffer)
            pins.remove(blockId)
            if (!pins.contains(blockId)) buffers.remove(blockId)
        }
    }

    fun unpinAll() {
        for (blockId in pins) {
            val buffer = buffers[blockId]
            if (buffer != null) bufferManager.unpin(buffer)
        }
        buffers.clear()
        pins.clear()
    }
}
