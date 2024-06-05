package simpledb.log

import org.example.simpledb.file.BlockId
import org.example.simpledb.file.FileMgr
import org.example.simpledb.file.Page

class LogMgr(
    private val fm: FileMgr,
    private val logfile: String
) {
    private val logPage: Page
    private val currentBlock: BlockId
    private var latestLSN = 0
    private var lastSavedLSN = 0
    init {
        val b = ByteArray(fm.blockSize)
        logPage = Page(b)
        currentBlock = when (val logSize = fm.length(logfile)) {
            0 -> appendNewBlock()
            else -> BlockId(logfile, logSize - 1)
        }
        fm.read(currentBlock, logPage)
    }
    fun flush(lsn: Int) {
        if (lsn >= lastSavedLSN) {
            forceFlush()
        }
    }
    fun iterator(): Iterator<ByteArray> {
        forceFlush()
        return LogIterator(fm, currentBlock)
    }
    private fun appendNewBlock(): BlockId {
        val blk = fm.append(logfile)
        logPage.setInt(0, fm.blockSize)
        fm.write(blk, logPage)
        return blk
    }
    private fun forceFlush() {
        fm.write(currentBlock, logPage)
        lastSavedLSN = latestLSN
    }
}