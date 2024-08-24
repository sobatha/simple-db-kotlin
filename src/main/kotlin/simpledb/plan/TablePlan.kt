package simpledb.plan

import simpledb.metadata.MetadataMgr
import simpledb.metadata.StatInfo
import simpledb.query.Scan
import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.record.TableScan
import simpledb.tx.Transaction

class TablePlan(
    private val transaction: Transaction,
    private val tableName: String,
    private val metadataManager: MetadataMgr,
) : Plan {
    private var layout: Layout = metadataManager.getLayout(tableName, transaction)
    private var statisticsInformation: StatInfo =
        metadataManager.getStatInfo(tableName, layout, transaction)

    override fun open(): Scan {
        return TableScan(transaction, tableName, layout)
    }

    override fun blocksAccessed(): Int {
        return statisticsInformation.blocksAccessed()
    }

    override fun recordsOutput(): Int {
        return statisticsInformation.recordsOutput()
    }

    override fun distinctValues(fieldName: String): Int {
        return statisticsInformation.distinctValues(fieldName)
    }

    override fun schema(): Schema {
        return layout.schema
    }
}