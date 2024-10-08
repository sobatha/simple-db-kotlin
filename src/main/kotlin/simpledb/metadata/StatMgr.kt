package simpledb.metadata

import simpledb.record.Layout
import simpledb.record.TableScan
import simpledb.tx.Transaction

/**
 * 統計情報を管理するクラス
 *
 * @property tableStatistics 各テーブルの統計情報
 * getメソッドで統計情報を返し、refresh/calcメソッドで統計情報を再計算する
 * @property numberCalls getStatisticsInformationが呼ばれるとインクリメントされる、100になると統計情報を再計算する
 */
class StatMgr(
    private val tableManager: TableMgr,
    private val tx: Transaction,
) {
    private val tableStatistics = mutableMapOf<String, StatInfo>()
    private var numberCalls = 0


    init {
        refreshStatistics(tx)
    }

    /**
     * [tableName]指定されたテーブル名の統計情報をStatisticsInformationクラスとして返す
     * numberCallsをインクリメントし、100を超える場合は統計情報を再計算する
     * @return 統計情報
     */
    @Synchronized
    fun getStatInfo(tableName: String, layout: Layout, transaction: Transaction): StatInfo {
        numberCalls += 1
        if (numberCalls > 100) refreshStatistics(transaction)
        var StatInfo = tableStatistics[tableName]
        if (StatInfo == null) {
            // 統計情報がない場合は統計情報を計算する
            StatInfo = calcTableStatistics(tableName, layout, transaction)
            tableStatistics[tableName] = StatInfo
        }
        return StatInfo
    }

    /**
     * テーブルカタログをループし、テーブルの統計情報を再計算する
     * 現在データベースで保持しているテーブルの統計情報をすべて再計算する
     */
    @Synchronized
    private fun refreshStatistics(transaction: Transaction) {
        numberCalls = 0
        val tableCatalogLayout = tableManager.getLayout("tablecatalog", transaction)
        val tableCatalog = TableScan(transaction, "tablecatalog", tableCatalogLayout)
        while (tableCatalog.next()) {
            val tableName = tableCatalog.getString("tablename")
            val layout = tableManager.getLayout(tableName, transaction)
            val statisticsInformation = calcTableStatistics(tableName, layout, transaction)
            tableStatistics[tableName] = statisticsInformation
        }
        tableCatalog.close()
    }

    /**
     * [tableName]指定されたテーブル名の[layout]スキーマ情報を受け取り、TableScanクラスによって
     * テーブルのレコード、ブロックの数を計算する
     * @return テーブルの統計情報
     */
    @Synchronized
    private fun calcTableStatistics(tableName: String, layout: Layout, transaction: Transaction): StatInfo {
        var numberRecords = 0
        var numberBlocks = 0
        val tableScan = TableScan(transaction, tableName, layout)
        while (tableScan.next()) {
            numberRecords += 1
            numberBlocks = tableScan.getRid().blockNumber + 1
        }
        tableScan.close()
        return StatInfo(numberBlocks, numberRecords)
    }
}