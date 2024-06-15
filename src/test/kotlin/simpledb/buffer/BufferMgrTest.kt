package simpledb.buffer

import simpledb.file.BlockId


fun main() {
    val db = simpledb.server.SimpleDB("buffertest")
    val bufferMgr = db.bufferMgr
    val buffs = MutableList<Buffer?>(5) { null }

    buffs[0] = bufferMgr.pin(BlockId("buffertest", 0))
    buffs[1] = bufferMgr.pin(BlockId("buffertest", 1))
    buffs[2] = bufferMgr.pin(BlockId("buffertest", 2))
    buffs[1]?.let { bufferMgr.unpin(it) }
    println("unpinned buffer 1 ${buffs[1]?.block}")

    buffs[3] = bufferMgr.pin(BlockId("buffertest", 3))
    buffs[4] = bufferMgr.pin(BlockId("buffertest", 4))
    println("available buffers: ${bufferMgr.numAvailable}")

    try {
        bufferMgr.pin(BlockId("buffertest", 3))
    } catch (e: Exception) {
        println("interrupted exception caught ${e.message}")
    }

}
