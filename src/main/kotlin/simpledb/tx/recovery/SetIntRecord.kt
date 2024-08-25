package simpledb.tx.recovery

import simpledb.tx.Transaction
import simpledb.file.BlockId
import simpledb.file.Page
import simpledb.log.LogMgr

class SetIntRecord(page: Page) : LogRecord {
    val txNum = page.getInt(Integer.BYTES)
    val fileName = page.getString(Integer.BYTES * 2)
    val blknum = page.getInt(Integer.BYTES * 2 + Page.maxLength(fileName.length))
    val offset = page.getInt(Integer.BYTES * 3 + Page.maxLength(fileName.length))
    val int_val = page.getInt(Integer.BYTES * 4 + Page.maxLength(fileName.length))
    val blk = BlockId(fileName, blknum)
//    init {
//        println("Log record setInt created with file name: $fileName")
//        println("Log record setInt created with offset: $offset")
//        println("Log record setInt created with string value: $int_val")
//        println("Log record setInt created with block: $blk")
//    }

    override val op: LogType
        get() = LogType.SETSTRING
    override val txNumber: Int
        get() = txNum

    override fun toString(): String = "<SETINT $txNum $blk $offset $int_val>"

    override fun undo(tx: Transaction) {
        tx.pin(blk)
        tx.setInt(blk, offset, int_val, false)
        tx.unpin(blk)
    }

    companion object {
        fun writeToLog(logMgr: LogMgr, txNum: Int, block: BlockId, offset: Int, value: Int): Int {
            val tpos = Integer.BYTES
            val fpos = tpos + Integer.BYTES
            val bpos = fpos + Page.maxLength(block.fileName.length)
            val opos = bpos + Integer.BYTES
            val vpos = opos + Integer.BYTES
            val rec_size = vpos + Integer.BYTES
            val rec = ByteArray(rec_size)
            val page = Page(rec)
            page.setInt(0, LogType.SETINT.value)
            page.setInt(tpos, txNum)
            page.setString(fpos, block.fileName)
            page.setInt(bpos, block.number)
            page.setInt(opos, offset)
            page.setInt(vpos, value)
//            println("<logType: SETINT, Tx number: $txNum>")
            return logMgr.append(rec)
        }
    }

}
