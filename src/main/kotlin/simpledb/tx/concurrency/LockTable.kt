package simpledb.tx.concurrency

import simpledb.file.BlockId
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

class LockTable {
    companion object {
        private const val MAX_TIME: Long = 10000
        private val lock = java.lang.Object()
    }

    private val locks: ConcurrentMap<BlockId, Int> = ConcurrentHashMap()

    private fun getLockVal(blk: BlockId): Int {
        return locks[blk] ?: 0
    }

    private fun waitingTooLong(startTime: Long) = System.currentTimeMillis() - startTime > MAX_TIME

    @Synchronized
    fun sLock(blk: BlockId) {
        try {
            val timestamp = System.currentTimeMillis()
            while (hasXlock(blk) && !waitingTooLong(timestamp)) {
                lock.wait(MAX_TIME)
            }
            if (hasXlock(blk)) throw RuntimeException("waiting too long to get slock")
            val lockVal = getLockVal(blk)
            locks[blk] = lockVal + 1
        } catch (e: Exception) {
            throw RuntimeException("cannot acquire lock")
        }
    }

    @Synchronized
    fun xLock(blk: BlockId) {
        try {
            val timestamp = System.currentTimeMillis()
            while (hasXlock(blk) && !waitingTooLong(timestamp)) {
                lock.wait(MAX_TIME)
            }
            if (hasOtherSlock(blk)) throw RuntimeException("waiting too long to get xlock")
            locks[blk] = -1
        } catch (e: Exception) {
            throw RuntimeException("cannot acquire lock")
        }
    }
    private fun hasXlock(blk: BlockId) = getLockVal(blk) < 0

    private fun hasOtherSlock(blk: BlockId) = getLockVal(blk) > 1

    fun unlock(blk: BlockId) {
        val lockVal = getLockVal(blk)
        if (lockVal > 1) {
            locks[blk] = lockVal - 1
        } else {
            locks.remove(blk)
        }
    }


}