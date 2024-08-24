package simpledb.metadata

import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.tx.Transaction

/**
 * テーブル、ビュー、インデックス、統計情報のメタデータを管理するクラス
 * メタデータを作成、保存するメソッド、取得するメソッドを持つ
 */
class MetadataMgr(
    private val isNew: Boolean,
    private val transaction: Transaction,
) {
    companion object {
        lateinit var tableMgr: TableMgr
        lateinit var viewMgr: ViewMgr
        lateinit var statisticsMgr: StatMgr
        lateinit var indexMgr: IndexMgr
    }

    init {
        tableMgr = TableMgr(isNew, transaction)
        viewMgr = ViewMgr(isNew, tableMgr, transaction)
        statisticsMgr = StatMgr(tableMgr, transaction)
        indexMgr = IndexMgr(isNew, tableMgr, statisticsMgr, transaction)
    }

    fun createTable(tableName: String, schema: Schema, tx: Transaction) {
        tableMgr.createTable(tableName, schema, tx)
    }

    fun getLayout(tableName: String, tx: Transaction): Layout {
        return tableMgr.getLayout(tableName, tx)
    }

    fun createView(viewName: String, viewDef: String, tx: Transaction) {
        viewMgr.createView(viewName, viewDef, tx)
    }

    fun getViewDef(viewName: String, tx: Transaction): String? {
        return viewMgr.getViewDef(viewName, tx)
    }

    fun createIndex(indexName: String, tableName: String, fieldName: String, tx: Transaction) {
        indexMgr.createIndex(indexName, tableName, fieldName, tx)
    }

    fun getIndexInformation(tableName: String, tx: Transaction): Map<String, IndexInfo> {
        return indexMgr.getIndexInfo(tableName, tx)
    }

    fun getStatisticsInformation(tableName: String, layout: Layout, tx: Transaction): StatInfo {
        return statisticsMgr.getStatInfo(tableName, layout, tx)
    }
}