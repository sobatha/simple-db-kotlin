package simpledb.buffer

import simpledb.file.BlockId
import simpledb.file.FileMgr
import simpledb.file.Page
import simpledb.log.LogMgr

class Buffer(private val fileMgr: FileMgr, private val logMgr: LogMgr) {
    val contents = Page(fileMgr.blockSize)
    var block: BlockId? = null
        private set
    private var pins = 0
    var modificationCount = 0
        private set
    private var lsn = 0
    val isPinned: Boolean
        get() = pins > 0

    fun setModified(modifiedNumber: Int, lsn: Int) {
        modificationCount = modifiedNumber
        if (lsn >= 0) this.lsn = lsn
    }

    fun assignToBlock(b: BlockId) {
        flush()
        block = b
        fileMgr.read(b, contents)
        pins = 0
    }

    fun flush() {
        if (modificationCount >= 0 && block != null) {
            logMgr.flush(lsn)
            fileMgr.write(block!!, contents)
            pins = 0
        }
    }

    fun pin() = pins++

    fun unpin() = pins--

}