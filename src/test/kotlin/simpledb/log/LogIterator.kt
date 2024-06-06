package simpledb.log

import org.example.simpledb.file.BlockId
import org.example.simpledb.file.FileMgr
import org.example.simpledb.file.Page

class LogIterator(
    private val fm: FileMgr,
    private var blk: BlockId
): Iterator<ByteArray> {
    val page = Page(ByteArray(fm.blockSize))
    var currentPos: Int = 0
    init {
        moveToBlock(blk)
    }
    private fun moveToBlock(blk: BlockId) {
        fm.read(blk, page)
        currentPos = page.getInt(0)
    }
    override fun hasNext() = currentPos < fm.blockSize || blk.number > 0
    override fun next(): ByteArray {
        if (currentPos == fm.blockSize) {
            blk = BlockId(blk.fileName, blk.number - 1)
            moveToBlock(blk)
        }
        val rec = page.getBytes(currentPos)
        currentPos += Int.SIZE_BYTES + rec.size
        return rec
    }

}