package simpledb.plan

import simpledb.metadata.MetadataMgr
import simpledb.parse.*
import simpledb.query.UpdateScan
import simpledb.simpledb.plan.UpdatePlanner
import simpledb.tx.Transaction

class BasicUpdatePlanner(
    private val metadataManager: MetadataMgr,
) : UpdatePlanner {

    override fun executeDelete(data: DeleteData, transaction: Transaction): Int {
        var plan: Plan = TablePlan(transaction, data.tableName, metadataManager)
        plan = SelectPlan(plan, data.predicate)
        val updateScan = plan.open() as UpdateScan
        var count = 0
        while (updateScan.next()) {
            updateScan.delete()
            count += 1
        }
        updateScan.close()
        return count
    }

    override fun executeModify(data: ModifyData, transaction: Transaction): Int {
        var plan: Plan = TablePlan(transaction, data.tableName, metadataManager)
        plan = SelectPlan(plan, data.predicate)
        val updateScan = plan.open() as UpdateScan
        var count = 0
        while (updateScan.next()) {
            val value = data.newValue.evaluate(updateScan)
            updateScan.setVal(data.fieldName, value)
            count += 1
        }
        updateScan.close()
        return count
    }

    override fun executeInsert(data: InsertData, transaction: Transaction): Int {
        val plan: Plan = TablePlan(transaction, data.tableName, metadataManager)
        val updateScan = plan.open() as UpdateScan
        updateScan.insert()
        val iterator = data.values.iterator()
        for (fieldName in data.fields) {
            val value = iterator.next()
            updateScan.setVal(fieldName, value)
        }
        updateScan.close()
        return 1
    }

    override fun executeCreateTable(data: CreateTableData, transaction: Transaction): Int {
        metadataManager.createTable(data.tableName, data.schema, transaction)
        return 0
    }

    override fun executeCreateView(data: CreateViewData, transaction: Transaction): Int {
        metadataManager.createView(data.viewName, data.viewDef(), transaction)
        return 0
    }

    override fun executeCreateIndex(data: CreateIndexData, transaction: Transaction): Int {
        metadataManager.createIndex(data.indexName, data.tableName, data.fieldName, transaction)
        return 0
    }
}