package simpledb.tx.recovery

import simpledb.tx.Transaction
import simpledb.buffer.Buffer
import simpledb.buffer.BufferMgr
import simpledb.log.LogMgr

class RecoveryMgr(
    private val transaction: Transaction,
    private val txNum: Int,
    private val logMgr: LogMgr,
    private val bufferMgr: BufferMgr,
) {
    init {
        StartRecord.writeToLog(logMgr, txNum)
    }

    fun commit() {
        bufferMgr.flushAll(txNum)
        val lsn = CommitRecord.writeToLog(logMgr, txNum)
        logMgr.flush(lsn)
    }

    fun rollback() {
        doRollback()
        bufferMgr.flushAll(txNum)
        val lsn = RollbackRecord.writeToLog(logMgr, txNum)
        logMgr.flush(lsn)
    }

    fun setInt(buffer: Buffer, offset: Int, value: Int): Int {
        val oldVal = buffer.contents.getInt(offset)
        val blk = buffer.block!!
        return SetIntRecord.writeToLog(logMgr, txNum, blk, offset, oldVal)
    }

    fun setString(buffer: Buffer, offset: Int, value: String): Int {
        val oldVal = buffer.contents.getString(offset)
        val blk = buffer.block!!
        return SetStringRecord.writeToLog(logMgr, txNum, blk, offset, oldVal)
    }

    fun recover() {
        doRecover()
        bufferMgr.flushAll(txNum)
        val lsn = CommitRecord.writeToLog(logMgr, txNum)
        logMgr.flush(lsn)
    }

    private fun doRecover() {
        val finishedTransactions: MutableList<Int> = mutableListOf()
        val iter = logMgr.iterator()
        while (iter.hasNext()) {
            val rec = LogRecord.createLogRecord(iter.next())
            when (rec.op) {
                LogType.CHECKPOINT -> return
                LogType.COMMIT -> finishedTransactions.add(rec.txNumber)
                LogType.ROLLBACK -> finishedTransactions.add(rec.txNumber)
                else -> {
                    if (!finishedTransactions.contains(rec.txNumber)) {
                        rec.undo(transaction)
                    }
                }
            }
        }
    }

    private fun doRollback() {
        val iter = logMgr.iterator()
        while (iter.hasNext()) {
            try {
                val rec = LogRecord.createLogRecord(iter.next())
                if (rec.txNumber == txNum) {
                    if (rec.op == LogType.START) {
                        return
                    }
                    rec.undo(transaction)
                }
            } catch (e: Exception) {
                e.printStackTrace()
            }

        }
    }
}