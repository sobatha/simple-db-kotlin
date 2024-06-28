package simpledb.buffer

import simpledb.file.BlockId
import simpledb.file.FileMgr
import simpledb.log.LogMgr

class BufferMgr(val fileMgr: FileMgr, val logMgr: LogMgr, var numBuff: Int) {
    private val bufferPool: MutableList<Buffer> = mutableListOf()
    @Volatile
    var numAvailable = numBuff
        private set

    private val MAX_TIME = 10000 // 10seconds for wait limits
    private val lock = java.lang.Object()

    init {
        bufferPool.addAll((0 until numBuff).map { Buffer(fileMgr, logMgr) })
    }

    fun waitingTooLong(startTime: Long) = System.currentTimeMillis() - startTime > MAX_TIME

    @Synchronized
    fun flushAll(modifiedCount: Int) {
        bufferPool
            .filter { it.modifyingTx == modifiedCount }
            .map { it.flush() }
    }

    @Synchronized
    fun unpin(buffer: Buffer) {
        buffer.unpin()
        if (!buffer.isPinned) {
            numAvailable++
        }
    }

    fun unpinAll() {
        bufferPool.forEach { unpin(it) }
    }

    fun getBuffer(blk: BlockId) = findExistingBuffer(blk)

    @Synchronized
    fun pin(blk: BlockId): Buffer {
        val timeStamp = System.currentTimeMillis()
        var buffer = tryToPin(blk)
        while (buffer == null && !waitingTooLong(timeStamp)) {
            lock.wait(MAX_TIME.toLong())
            buffer = tryToPin(blk)
        }
        return buffer ?: throw InterruptedException()
    }

    @Synchronized
    fun tryToPin(blk: BlockId):Buffer? {
        val buffer = findExistingBuffer(blk)
            ?: run {
                chooseUnpinnedBuffer()
                ?.apply { assignToBlock(blk) }
                ?: return null
            }

        return buffer.apply {
            if(!isPinned) numAvailable--
            pin()
        }
    }

    private fun chooseUnpinnedBuffer(): Buffer? {
        return bufferPool.find { !it.isPinned }
    }

    private fun findExistingBuffer(blk: BlockId): Buffer? {
        return bufferPool.find { it.block == blk && it.block != null }
    }
}