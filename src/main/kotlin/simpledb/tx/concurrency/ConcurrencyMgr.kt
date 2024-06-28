package org.example.simpledb.tx.concurrency

import simpledb.file.BlockId
import simpledb.tx.concurrency.LockTable

class ConcurrencyMgr {
    companion object {
        val lockTable = LockTable()
        val locks: MutableMap<BlockId, String> = mutableMapOf()
    }
}