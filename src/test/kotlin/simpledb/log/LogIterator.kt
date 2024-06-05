package simpledb.log

import org.example.simpledb.file.BlockId
import org.example.simpledb.file.FileMgr

class LogIterator(
    private val fm: FileMgr,
    private val blk: BlockId
): Iterator<ByteArray> {
    override fun hasNext(): Boolean {
        TODO("Not yet implemented")
    }

    override fun next(): ByteArray {
        TODO("Not yet implemented")
    }

}