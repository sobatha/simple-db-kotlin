package simpledb.parse

import simpledb.query.Constant
import simpledb.query.Expression
import simpledb.query.Predicate
import simpledb.record.Schema

data class IndexData(
    val indexName: String,
    val tableName: String,
    val fieldName: String,
)

data class CreateTableData(
    val tableName: String,
    val schema: Schema,
)

class CreateViewData(
    val viewName: String,
    private val queryData: QueryData,
) {
    fun viewDef(): String {
        return queryData.toString()
    }
}

data class CreateIndexData(
    val indexName: String,
    val tableName: String,
    val fieldName: String,
)

data class DeleteData(
    val tableName: String,
    val predicate: Predicate,
)

data class InsertData(
    val tableName: String,
    val fields: List<String>,
    val values: List<Constant>,
)

class ModifyData(
    val tableName: String,
    val fieldName: String,
    val newValue: Expression,
    val predicate: Predicate,
)

class QueryData(
    val fields: List<String>,
    val tables: Collection<String>,
    val predicate: Predicate,
) {
    override fun toString(): String {
        var result = "select "
        for (filedName in fields) {
            result += "$filedName, "
        }
        result = result.substring(0, result.length-2) //zap final comma
        result += " from "
        for (tableName in tables) {
            result += "$tableName, "
        }
        result = result.substring(0, result.length-2) // zap final comma
        val predicateString = predicate.toString()
        if (predicateString != "") {
            result += " where $predicateString"
        }
        return result
    }
}