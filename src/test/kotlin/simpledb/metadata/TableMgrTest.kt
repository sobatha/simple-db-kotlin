package simpledb.metadata

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import simpledb.metadata.TableMgr
import simpledb.record.Schema
import simpledb.record.TableScan
import simpledb.server.SimpleDB

class TableMgrTest : FunSpec({
    test("test table manager") {
        val db = SimpleDB("tabletest")
        val tx = db.newTransaction()
        val tm = TableMgr(true, tx)

        val schema = Schema().apply {
            addIntField("A")
            addStringField("B", 9)
        }
        tm.createTable("MyTable", schema, tx)

        println("Here are all the tables and their lengths.")
        val tcatLayout = tm.getLayout("tableCatalog", tx)
        var ts = TableScan(tx, "tableCatalog", tcatLayout)
        while (ts.next()) {
            val tname = ts.getString("tableName")
            val slotsize = ts.getInt("slotSize")
            println("$tname $slotsize")
        }
        ts.close()

        println("\nHere are the fields for each table and their offsets")
        val fcatLayout = tm.getLayout("fieldCatalog", tx)
        ts = TableScan(tx, "fieldCatalog", fcatLayout)
        while (ts.next()) {
            val tname = ts.getString("tableName")
            val fname = ts.getString("fieldName")
            val offset = ts.getInt("offset")
            println("$tname $fname $offset")
        }
        ts.close()
    }
}


)
