package simpledb.record

import simpledb.file.Page

class Layout {
    val schema: Schema
    private var offsets: MutableMap<String, Int> = mutableMapOf()
    val slotSize: Int

    constructor(schema: Schema) {
        this.schema = schema
        var pos = Int.SIZE_BYTES // leave space for the empty/inuse flag
        schema.fields.forEach { name ->
            offsets[name] = pos
            pos += lengthInBytes(name)
        }
        slotSize = pos
    }

    constructor(schema: Schema, offsets: MutableMap<String, Int>, slotSize: Int) {
        this.schema = schema
        this.offsets = offsets
        this.slotSize = slotSize
    }


    fun offset(name: String) = offsets[name]

    private fun lengthInBytes(name: String) =
        when (schema.type(name)) {
            FieldType.INTEGER -> Int.SIZE_BYTES
            FieldType.VARCHAR -> Page.maxLength(schema.length(name))
        }
}