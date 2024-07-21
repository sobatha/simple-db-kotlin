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
    fun getStatInfo(tblname: String, layout: Layout, tx: Transaction): StatInfo {
        numcalls++
        if (numcalls > 100) refreshStatistics(tx)
        return tablestats[tblname] ?: calcTableStats(tblname, layout, tx).also {
            tablestats[tblname] = it
        }
    }

    @Synchronized
    private fun refreshStatistics(tx: Transaction) {
        tablestats = mutableMapOf()
        numcalls = 0
        val tcatlayout = tblMgr.getLayout("tblcat", tx)
        TableScan(tx, "tblcat", tcatlayout).let { tcat ->
            while (tcat.next()) {
                val tblname = tcat.getString("tblname")
                val layout = tblMgr.getLayout(tblname, tx)
                val si = calcTableStats(tblname, layout, tx)
                tablestats[tblname] = si
            }
            tcat.close()
        }
    }

    @Synchronized
    private fun calcTableStats(tblname: String, layout: Layout, tx: Transaction): StatInfo {
        var numRecs = 0
        var numblocks = 0
        TableScan(tx, tblname, layout).let { ts ->
            while (ts.next()) {
                numRecs++
                numblocks = ts.getRid().blockNumber + 1
            }
            ts.close()
        }
        return StatInfo(numblocks, numRecs)
    }
}
