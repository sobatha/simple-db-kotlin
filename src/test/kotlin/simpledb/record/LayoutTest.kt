package simpledb.record

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.file.Page

class LayoutTest : FunSpec({
    test("calculate offsets for fields in schema") {
        val schema = Schema().apply {
            addIntField("A")
            addStringField("B", 8)
        }
        val layout = Layout(schema)


        val actualOffsetA = layout.offset("A")
        val actualOffsetB = layout.offset("B")
        val slotSize = layout.slotSize

        print("A: $actualOffsetA, B: $actualOffsetB")

        actualOffsetA shouldBe 4
        actualOffsetB shouldBe 8
        slotSize shouldBe 36
    }
})
