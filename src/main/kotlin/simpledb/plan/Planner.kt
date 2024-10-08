package simpledb.plan

import simpledb.tx.Transaction
import simpledb.parse.*
import simpledb.simpledb.plan.*

class Planner(
    private val queryPlanner: QueryPlanner,
    private val updatePlanner: UpdatePlanner,
) {

    fun createQueryPlan(cmd: String, transaction: Transaction): Plan {
        val parser = Parser(cmd)
        val queryData = parser.query()

        return queryPlanner.createPlan(queryData, transaction)
    }

    fun executeUpdate(cmd: String, transaction: Transaction): Int {
        val parser = Parser(cmd)
        // code to verify the update command should be here...
        return when (val updateData = parser.updateCmd()) {
            is InsertData -> {
                updatePlanner.executeInsert(updateData, transaction)
            }
            is DeleteData -> {
                updatePlanner.executeDelete(updateData, transaction)
            }
            is ModifyData -> {
                updatePlanner.executeModify(updateData, transaction)
            }
            is CreateTableData -> {
                updatePlanner.executeCreateTable(updateData, transaction)
            }
            is CreateViewData -> {
                updatePlanner.executeCreateView(updateData, transaction)
            }
            is CreateIndexData -> {
                updatePlanner.executeCreateIndex(updateData, transaction)
            }
            else -> {
                0
            }
        }
    }
}