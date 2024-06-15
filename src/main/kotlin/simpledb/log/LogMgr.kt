package simpledb.log

import simpledb.file.BlockId
import simpledb.file.FileMgr
import simpledb.file.Page

class LogMgr(
    private val fm: FileMgr,
    private val logfile: String
) {
    private val logPage: Page
    private var currentBlock: BlockId
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
    @Synchronized
    fun append(rec: ByteArray): Int {
        var boundary = logPage.getInt(0)
        val recSize = rec.size
        val bytesNeeded = recSize + Int.SIZE_BYTES
        if (boundary - bytesNeeded < Int.SIZE_BYTES) {
            forceFlush()
            currentBlock = appendNewBlock()
            boundary = logPage.getInt(0)
        }
        val recPos = boundary - bytesNeeded
        logPage.setBytes(recPos, rec)
        logPage.setInt(0, recPos)
        latestLSN += 1
        return latestLSN
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