package simpledb.metadata

import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.record.TableScan
import simpledb.tx.Transaction
import java.lang.RuntimeException

const val MAX_NAME = 16

class TableMgr(
    private val isNew: Boolean,
    private val transaction: Transaction,
) {
    private var tableCatalogLayout: Layout
    private var fieldCatalogLayout: Layout

    init {
        val tableCatalogSchema = Schema()
        tableCatalogSchema.addStringField("tablename", MAX_NAME)
        tableCatalogSchema.addIntField("slotsize")
        tableCatalogLayout = Layout(tableCatalogSchema)

        val fieldCatalogSchema = Schema()
        fieldCatalogSchema.addStringField("tablename", MAX_NAME)
        fieldCatalogSchema.addStringField("fieldname", MAX_NAME)
        fieldCatalogSchema.addIntField("type")
        fieldCatalogSchema.addIntField("length")
        fieldCatalogSchema.addIntField("offset")
        fieldCatalogLayout = Layout(fieldCatalogSchema)

        if (isNew) {
            createTable("tablecatalog", tableCatalogSchema, transaction)
            createTable("fieldcatalog", fieldCatalogSchema, transaction)
        }
    }

    fun createTable(tableName: String, schema: Schema, tx: Transaction) {
        val layout = Layout(schema)

        val tableCatalog = TableScan(tx, "tablecatalog", tableCatalogLayout)
        tableCatalog.insert()
        tableCatalog.setString("tablename", tableName)
        tableCatalog.setInt("slotsize", layout.slotSize())
        tableCatalog.close()

        val fieldCatalog = TableScan(tx, "fieldcatalog", fieldCatalogLayout)
        for (fieldName in schema.fields) {
            fieldCatalog.insert()
            fieldCatalog.setString("tablename", tableName)
            fieldCatalog.setString("fieldname", fieldName)
            val schemaType = schema.type(fieldName) ?: throw RuntimeException("null schema type")
            fieldCatalog.setInt("type", schemaType)
            val schemaLength = schema.length(fieldName) ?: throw RuntimeException("null schema type")
            fieldCatalog.setInt("length", schemaLength)
            val layoutOffset = layout.offset(fieldName) ?: throw RuntimeException("null schema type")
            fieldCatalog.setInt("offset", layoutOffset)
        }
        fieldCatalog.close()
    }

    fun getLayout(tableName: String, tx: Transaction): Layout {
        // テーブルのスロットサイズ
        var size = -1
        val tableCatalog = TableScan(tx, "tablecatalog", tableCatalogLayout)
        while (tableCatalog.next()) {
            if (tableCatalog.getString("tablename") == tableName) {
                size = tableCatalog.getInt("slotsize")
                break
            }
        }
        tableCatalog.close()
        val schema = Schema()
        val offsets = mutableMapOf<String, Int>()
        val fieldCatalog = TableScan(tx, "fieldcatalog", fieldCatalogLayout)
        while (fieldCatalog.next()) {
            if (fieldCatalog.getString("tablename") == tableName) {
                val fieldName = fieldCatalog.getString("fieldname")
                val fieldType = fieldCatalog.getInt("type")
                val fieldLength = fieldCatalog.getInt("length")
                val offset = fieldCatalog.getInt("offset")
                offsets[fieldName] = offset
                schema.addField(fieldName, fieldType, fieldLength)
            }
        }
        fieldCatalog.close()
        return Layout(schema, offsets, size)
    }
}
