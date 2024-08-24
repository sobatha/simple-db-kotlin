package simpledb.metadata

import simpledb.server.SimpleDB
import simpledb.record.*
import kotlin.math.roundToInt

fun main() {
    val db = SimpleDB("metadatamgrtest")
    val tx = db.newTransaction()
    val mdm = MetadataMgr(true, tx)

    val sch = Schema().apply {
        addIntField("A")
        addStringField("B", 9)
    }

    // Part 1: Table Metadata
    mdm.createTable("MyTable", sch, tx)
    val layout = mdm.getLayout("MyTable", tx)
    val size = layout.slotSize
    val sch2 = layout.schema
    println("MyTable has slot size $size")
    println("Its fields are:")
    for (fldname in sch2.fields) {
        val type = if (sch2.type(fldname) == FieldType.INTEGER) {
            "int"
        } else {
            val strlen = sch2.length(fldname)
            "varchar($strlen)"
        }
        println("$fldname: $type")
    }

    // Part 2: Statistics Metadata
    TableScan(tx, "MyTable", layout).let { ts ->
        repeat(50) {
            ts.insert()
            val n = (Math.random() * 50).roundToInt()
            ts.setInt("A", n)
            ts.setString("B", "rec$n")
        }
        ts.close()
    }
    val si = mdm.getStatInfo("MyTable", layout, tx)
    println("B(MyTable) = ${si.blocksAccessed()}")
    println("R(MyTable) = ${si.recordsOutput()}")
    println("V(MyTable,A) = ${si.distinctValues("A")}")
    println("V(MyTable,B) = ${si.distinctValues("B")}")

    // Part 3: View Metadata
    val viewdef = "select B from MyTable where A = 1"
    mdm.createView("viewA", viewdef, tx)
    val v = mdm.getViewDef("viewA", tx)
    println("View def = $v")

    // Part 4: Index Metadata
    mdm.createIndex("indexA", "MyTable", "A", tx)
    mdm.createIndex("indexB", "MyTable", "B", tx)
    val idxmap = mdm.getIndexInfo("MyTable", tx)

    idxmap["A"]?.let { ii ->
        println("B(indexA) = ${ii.blocksAccessed()}")
        println("R(indexA) = ${ii.recordsOutput()}")
        println("V(indexA,A) = ${ii.distinctValues("A")}")
        println("V(indexA,B) = ${ii.distinctValues("B")}")
    }

    idxmap["B"]?.let { ii ->
        println("B(indexB) = ${ii.blocksAccessed()}")
        println("R(indexB) = ${ii.recordsOutput()}")
        println("V(indexB,A) = ${ii.distinctValues("A")}")
        println("V(indexB,B) = ${ii.distinctValues("B")}")
    }

    tx.commit()
}