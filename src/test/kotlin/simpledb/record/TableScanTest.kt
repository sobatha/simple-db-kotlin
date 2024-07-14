package simpledb.record

import simpledb.server.SimpleDB

object TableScanTest {
    @JvmStatic
    fun main(args: Array<String>) {
        val db = SimpleDB("tabletest")
        val tx = db.newTx()

        val sch = Schema().apply {
            addIntField("A")
            addStringField("B", 9)
        }
        val layout = Layout(sch)

        for (fldname in layout.schema.fields) {
            val offset = layout.offset(fldname)
            println("$fldname has offset $offset")
        }

        println("Filling the table with 50 random records.")
        val tableScan = TableScan(tx, "T", layout)
        for (i in 0 until 50) {
            tableScan.insert()
            val n = (Math.random() * 50).roundToInt()
            tableScan.setInt("A", n)
            tableScan.setString("B", "rec$n")
            println("inserting into slot ${tableScan.getRid()}: {$n, rec$n}")
        }

        println("Deleting these records, whose A-values are less than 25.")
        var count = 0
        tableScan.beforeFirst()
        while (tableScan.next()) {
            val a = tableScan.getInt("A")
            val b = tableScan.getString("B")
            if (a < 25) {
                count++
                println("slot ${tableScan.getRid()}: {$a, $b}")
                tableScan.delete()
            }
        }
        println("$count values under 25 were deleted.\n")

        println("Here are the remaining records.")
        tableScan.beforeFirst()
        while (tableScan.next()) {
            val a = tableScan.getInt("A")
            val b = tableScan.getString("B")
            println("slot ${tableScan.getRid()}: {$a, $b}")
        }
        tableScan.close()
        tx.commit()
    }

    private fun Double.roundToInt(): Int {
        return Math.round(this).toInt()
    }
}

