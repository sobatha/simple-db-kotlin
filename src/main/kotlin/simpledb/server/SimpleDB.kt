package simpledb.server

import simpledb.file.FileMgr
import simpledb.log.LogMgr
import simpledb.metadata.MetadataMgr
import simpledb.plan.BasicQueryPlanner
import simpledb.plan.BasicUpdatePlanner
import simpledb.plan.Planner
import simpledb.simpledb.plan.QueryPlanner
import simpledb.simpledb.plan.UpdatePlanner
import simpledb.tx.Transaction
import java.io.File


class SimpleDB(private val dirName: String) {
    companion object {
        const val BUFFER_SIZE = 8
        const val BLOCK_SIZE = 400
        const val LOG_FILE = "logfile.log"
    }

    val dbDirectory = File(dirName)
    val fileMgr = FileMgr(dbDirectory, BLOCK_SIZE)
    val logMgr = LogMgr(fileMgr, LOG_FILE)
    val bufferMgr = simpledb.buffer.BufferMgr(fileMgr, logMgr, BUFFER_SIZE)
    lateinit var planner: Planner

    init {
        val transaction = newTransaction()
        val isNew = fileMgr.isNew
        if (isNew) {
            println("creating new database")
        } else {
            println("db already exist")
            transaction.recover()
        }
        val metadataManager = MetadataMgr(isNew, transaction)
        val queryPlanner: QueryPlanner = BasicQueryPlanner(metadataManager)
        val updatePlanner: UpdatePlanner = BasicUpdatePlanner(metadataManager)
        planner = Planner(queryPlanner, updatePlanner)
        transaction.commit()
    }

    fun newTransaction(): Transaction {
        return Transaction(fileMgr, bufferMgr, logMgr)
    }
}