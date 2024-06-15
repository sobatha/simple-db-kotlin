package org.example.simpledb.tx.recovery

import org.example.simpledb.file.Page

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
    }
}

enum class LogType(val value: Int) {
    START(0), COMMIT(1), ROLLBACK(2), SETINT(3), SETSTRING(4), CHECKPOINT(5)
}