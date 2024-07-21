package simpledb.metadata

import simpledb.record.FieldType
import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.record.TableScan
import simpledb.tx.recovery.Transaction

class TableMgr(private val isNew: Boolean, val transaction: Transaction) {
    private val tableCatalogLayout: Layout
    private val fieldCatalogLayout: Layout

    init {
        val tableCatalogSchema = Schema().apply {
            addStringField("tableName", MAX_NAME)
            addIntField("slotSize")
        }
        tableCatalogLayout = Layout(tableCatalogSchema)

        val fieldCatalogSchema = Schema().apply {
            addStringField("tableName", MAX_NAME)
            addStringField("fieldName", MAX_NAME)
            addIntField("type")
            addIntField("length")
            addIntField("offset")
        }
        fieldCatalogLayout = Layout(fieldCatalogSchema)

        if (isNew) {
            createTable("tableCatalog", tableCatalogSchema, transaction)
            createTable("fieldCatalog", fieldCatalogSchema, transaction)
        }
    }

    fun createTable(tableName: String, schema: Schema, transaction: Transaction) {
        val layout = Layout(schema)
        TableScan(transaction, "tableCatalog", tableCatalogLayout).apply {
            insert()
            setString("tableName", tableName)
            setInt("slotSize", layout.slotSize)
            close()
        }

        val tableScan = TableScan(transaction, "fieldCatalog", fieldCatalogLayout)
        schema.fields.forEach { field ->
            tableScan.apply {
                insert()
                setString("tableName", tableName)
                setString("fieldName", field)
                setInt("type", schema.type(field).number)
                setInt("length", schema.length(field))
                setInt("offset", layout.offset(field)!!)
            }
        }
        tableScan.close()
    }

    fun getLayout(tableName: String, transaction: Transaction): Layout {
        var size = -1
        val tableCatalog = TableScan(transaction, "tableCatalog", tableCatalogLayout)
        while (tableCatalog.next()) {
            if (tableCatalog.getString("tableName") == tableName) {
                size = tableCatalog.getInt("slotSize")
                break
            }
        }
        val schema = Schema()
        val offsets = mutableMapOf<String, Int>()
        val fieldCatalog = TableScan(transaction, "fieldCatalog", fieldCatalogLayout)
        while (fieldCatalog.next()) {
            if (fieldCatalog.getString("tableName") == tableName) {
                val fieldName = fieldCatalog.getString("fieldName")
                val fieldLength = fieldCatalog.getInt("length")
                val fieldType = fieldCatalog.getInt("type")
                val offset = fieldCatalog.getInt("offset")
                offsets[fieldName] = offset
                schema.addField(fieldName, FieldType.fieldTypeFactory(fieldType), fieldLength)
            }
        }
        fieldCatalog.close()
        return Layout(schema, offsets, size)
    }

    companion object {
        const val MAX_NAME = 16
    }
}