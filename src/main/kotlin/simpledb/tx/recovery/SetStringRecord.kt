package org.example.simpledb.tx.recovery

import org.example.simpledb.file.BlockId
import org.example.simpledb.file.Page
import simpledb.log.LogMgr

class SetStringRecord(val page: Page) : LogRecord {
    val txNum = page.getInt(Integer.BYTES)
    val fileName = page.getString(Integer.BYTES * 2)
    val blknum = page.getInt(Integer.BYTES * 2 + fileName.length)
    val offset = page.getInt(Integer.BYTES * 3 + fileName.length)
    val string_val = page.getString(Integer.BYTES * 4 + fileName.length)
    val blk = BlockId(fileName, blknum)

    override val op: LogType
        get() = LogType.SETSTRING
    override val txNumber: Int
        get() = txNum

    override fun toString(): String = "<SETSTRING $txNum $blk $offset $string_val>"

    override fun undo(tx: Transaction) {
        tx.pin(blk)
        tx.setString(blk, offset, string_val, false)
        tx.unpin(blk)
    }

    companion object {
        fun writeToLog(logMgr: LogMgr, txNum: Int, block: BlockId, offset: Int, string_val: String): Int {
            val tpos = Integer.BYTES
            val fpos = tpos + Integer.BYTES
            val bpos = fpos + block.fileName.length
            val opos = bpos + Integer.BYTES
            val vpos = opos + Integer.BYTES
            val rec_size = vpos + Page.maxLength(string_val.length)
            val rec = ByteArray(rec_size)
            val page = Page(rec)
            page.setInt(0, LogType.SETSTRING.value)
            page.setInt(tpos, txNum)
            page.setString(fpos, block.fileName)
            page.setInt(bpos, block.number)
            page.setInt(opos, offset)
            page.setString(vpos, string_val)
            return logMgr.append(rec)
        }
    }
}
