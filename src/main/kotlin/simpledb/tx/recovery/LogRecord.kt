package simpledb.tx.recovery

import simpledb.file.Page
import simpledb.log.LogMgr

interface LogRecord {
    val op: LogType
    val txNumber: Int
    fun undo(tx: Transaction)

    companion object {
        fun createLogRecord(bytes: ByteArray): LogRecord {
            val p = Page(bytes)
            return when (val op = p.getInt(0)) {
                LogType.START.value -> StartRecord(p)
                LogType.COMMIT.value -> CommitRecord(p)
                LogType.ROLLBACK.value -> RollbackRecord(p)
                LogType.SETINT.value -> SetIntRecord(p)
                LogType.SETSTRING.value -> SetStringRecord(p)
                LogType.CHECKPOINT.value -> CheckpointRecord(p)
                else -> throw IllegalArgumentException("Unknown log record type $op")
            }
        }

        fun writeToLog(logMgr: LogMgr, txNum: Int, logType: LogType): Int {
            val tpos = Integer.BYTES
            val fpos = tpos + Integer.BYTES
            val bpos = fpos + Page.maxLength("dummyfile".length)
            val opos = bpos + Integer.BYTES
            val vpos = opos + Integer.BYTES
            val rec_size = vpos + Integer.BYTES
            val rec = ByteArray(rec_size)
            val page = Page(rec)
            page.setInt(0, logType.value)
            page.setInt(tpos, txNum)
            page.setString(fpos, "dummyfile")
            page.setInt(bpos, 0)
            page.setInt(opos, 0)
            page.setInt(vpos, 0)
            return logMgr.append(rec)
        }
    }
}

enum class LogType(val value: Int) {
    START(0), COMMIT(1), ROLLBACK(2), SETINT(3), SETSTRING(4), CHECKPOINT(5)
}