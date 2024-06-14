package simpledb.buffer

import org.example.simpledb.file.BlockId
import org.example.simpledb.file.FileMgr
import simpledb.log.LogMgr

class BufferMgr(val fileMgr: FileMgr, val logMgr: LogMgr, var numBuff: Int) {
    private val bufferPool: MutableList<Buffer> = mutableListOf()
    @Volatile
    var numAvailable = 0
        private set

    private val MAX_TIME = 10000 // 10seconds for wait limits
    private val lock = java.lang.Object()

    fun waitingTooLong(startTime: Long) = System.currentTimeMillis() - startTime > MAX_TIME

    @Synchronized
    fun flushAll(modifiedCount: Int) {
        bufferPool
            .filter { it.modificationCount == modifiedCount }
            .map { it.flush() }
    }

    @Synchronized
    fun unpin(buffer: Buffer) {
        buffer.unpin()
        if (!buffer.isPinned) {
            numAvailable++
        }
    }
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
        val buffer = findExistingBuffer(blk) ?: run {
            chooseUnpinnedBuffer() ?: return null
        }
        return buffer.apply {
            if(!isPinned) numAvailable--
            pin()
        }
    }

    private fun chooseUnpinnedBuffer(): Buffer? {
        return bufferPool.find { !it.isPinned }
    }

    private fun findExistingBuffer(block: BlockId): Buffer? {
        return bufferPool.find { it.block == block && it.block != null }
    }
}