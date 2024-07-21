package org.example.simpledb.index.hash

import simpledb.record.Layout
import simpledb.tx.recovery.Transaction

class HashIndex(tx: Transaction, idxname: String, idxLayout: Layout) {
    companion object {
        fun searchCost(numblocks: Int, rpb: Int): Int {
            TODO()
        }
    }

}
