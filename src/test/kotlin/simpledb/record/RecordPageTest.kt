package simpledb.record

import io.kotest.core.spec.style.FunSpec
import simpledb.file.BlockId
import simpledb.server.SimpleDB
import simpledb.tx.Transaction
class RecordTest : FunSpec({

    lateinit var db: SimpleDB
    lateinit var tx: Transaction
    lateinit var layout: Layout
    lateinit var blk: BlockId
    lateinit var rp: RecordPage

    beforeTest {
        db = SimpleDB("recordTest")
        tx = db.newTransaction()

        val schema = Schema().apply {
            addIntField("A")
            addStringField("B", 9)
        }
        layout = Layout(schema)

        blk = tx.append("testfile")
        rp = RecordPage(tx, blk, layout)
        rp.format()
    }

    test("Fill the page with random records") {
        println("Filling the page with random records.")
        var slot = rp.insertAfter(-1)
        while (slot >= 0) {
            val n = (Math.random() * 50).toInt()
            rp.setInt(slot, "A", n)
            rp.setString(slot, "B", "rec$n")
            println("inserting into slot $slot: {$n, rec$n}")
            slot = rp.insertAfter(slot)
        }

        println("Deleting these records, whose A-values are less than 25.")
        var count = 0
        slot = rp.nextAfter(-1)
        while (slot >= 0) {
            val a = rp.getInt(slot, "A")!!
            val b = rp.getString(slot, "B")
            if (a < 25) {
                count++
                println("slot $slot: {$a, $b}")
                rp.delete(slot)
            }
            slot = rp.nextAfter(slot)
        }
        println("$count values under 25 were deleted.\n")

        println("Here are the remaining records.")
        slot = rp.nextAfter(-1)
        while (slot >= 0) {
            val a = rp.getInt(slot, "A")
            val b = rp.getString(slot, "B")
            println("slot $slot: {$a, $b}")
            slot = rp.nextAfter(slot)
        }
    }

    afterTest {
        tx.unpin(blk)
        tx.commit()
    }
})

