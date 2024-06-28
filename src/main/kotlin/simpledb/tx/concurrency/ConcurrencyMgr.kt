package org.example.simpledb.tx.concurrency

import simpledb.file.BlockId
import simpledb.tx.concurrency.LockTable

class ConcurrencyMgr {
    companion object {
        val lockTable = LockTable()
    }
    val locks: MutableMap<BlockId, String> = mutableMapOf()

    fun sLock(blk: BlockId) {
        if (!locks.containsKey(blk)) {
            lockTable.sLock(blk)
            locks[blk] = "S"
        }
    }
    private fun hasXlock(blk: BlockId): Boolean =
        locks.getOrDefault(blk, null) == "X"
    fun xLock(blk: BlockId) {
        if (!hasXlock(blk)) {
            sLock(blk)
            lockTable.xLock(blk)
            locks[blk] = "X"
        }
    }
    fun release() {
        locks.keys.forEach{ lockTable.unlock(it) }
        locks.clear()
    }

}