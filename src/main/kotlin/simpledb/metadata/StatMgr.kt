package simpledb.metadata

import simpledb.tx.recovery.Transaction
import simpledb.record.*

class StatMgr(private val tblMgr: TableMgr, tx: Transaction) {
    private var tablestats: MutableMap<String, StatInfo> = mutableMapOf()
    private var numcalls: Int = 0

    init {
        refreshStatistics(tx)
    }

    @Synchronized
    fun getStatInfo(tableName: String, layout: Layout, tx: Transaction): StatInfo {
        numcalls++
        if (numcalls > 100) refreshStatistics(tx)
        return tablestats[tableName] ?: calcTableStats(tableName, layout, tx).also {
            tablestats[tableName] = it
        }
    }

    @Synchronized
    private fun refreshStatistics(tx: Transaction) {
        tablestats = mutableMapOf()
        numcalls = 0
        val tcatlayout = tblMgr.getLayout("tableCatalog", tx)
        TableScan(tx, "tableCatalog", tcatlayout).let { tcat ->
            while (tcat.next()) {
                val tableName = tcat.getString("tableName")
                val layout = tblMgr.getLayout(tableName, tx)
                val si = calcTableStats(tableName, layout, tx)
                tablestats[tableName] = si
            }
            tcat.close()
        }
    }

    @Synchronized
    private fun calcTableStats(tableName: String, layout: Layout, tx: Transaction): StatInfo {
        var numRecs = 0
        var numblocks = 0
        TableScan(tx, tableName, layout).let { ts ->
            while (ts.next()) {
                numRecs++
                numblocks = ts.getRid().blockNumber + 1
            }
            ts.close()
        }
        return StatInfo(numblocks, numRecs)
    }
}
