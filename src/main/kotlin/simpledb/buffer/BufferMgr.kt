package simpledb.buffer

import simpledb.file.BlockId
import simpledb.file.FileMgr
import simpledb.log.LogMgr

class BufferMgr(
    val fm: FileMgr,
    val lm: LogMgr,
    var numBuffers: Int,
) {
    private val bufferPool: MutableList<Buffer> = mutableListOf<Buffer>()
    private var numAvailable = numBuffers
    private val MAX_TIME: Long = 10000 // 10 seconds
    private val lock = java.lang.Object()

    init {
        for (i in 0..numBuffers) {
            bufferPool.add(i, Buffer(fm, lm))
        }
    }

    @Synchronized
    fun available(): Int {
        return numAvailable
    }

    @Synchronized
    fun flushAll(txnum: Int) {
        for (buffer in bufferPool) {
            if (buffer.modifyingTx() == txnum) buffer.flush()
        }
    }

    fun unpin(buffer: Buffer) {
        synchronized(lock) {
            buffer.unpin()
            if (!buffer.isPinned()) {
                // bufferが使用できる場合は使用できる数（numAvailable）を増やし、スレッドを開放
                numAvailable++
                lock.notifyAll()
            }
        }
    }

    fun pin(blockId: BlockId): Buffer {
        synchronized(lock) {
            try {
                val timestamp = System.currentTimeMillis()
                var buffer = tryToPin(blockId)
                while (buffer == null && !waitingTooLong(timestamp)) {
                    lock.wait(MAX_TIME)
                    buffer = tryToPin(blockId)
                }
                if (buffer == null) throw Exception()

                return buffer
            } catch (e: InterruptedException) {
                throw Exception()
            }
        }
    }

    private fun waitingTooLong(starttime: Long): Boolean {
        return System.currentTimeMillis() - starttime > MAX_TIME
    }

    private fun tryToPin(blockId: BlockId): Buffer? {
        var buffer = findExistingBuffer(blockId)
        if (buffer == null) {
            buffer = chooseUnpinnedBuffer()
            if (buffer == null) return null
            buffer.assignToBlock(blockId)
        }
        if (!buffer.isPinned()) numAvailable--
        buffer.pin()
        return buffer
    }

    private fun findExistingBuffer(blockId: BlockId): Buffer? {
        for (buffer in bufferPool) {
            val bId = buffer.blockId()
            if (bId == blockId) {
                return buffer
            }
        }
        return null
    }

    private fun chooseUnpinnedBuffer(): Buffer? {
        for (buffer in bufferPool) {
            if (!buffer.isPinned()) return buffer
        }
        return null
    }
}