package org.example.simpledb.record

import simpledb.file.Page

class Layout(val schema: Schema) {
    private val offsets: MutableMap<String, Int> = mutableMapOf()
    val slotSize: Int

    init {
        var pos = Int.SIZE_BYTES
        schema.fields.forEach{ name ->
            offsets[name] = pos
            pos += lengthInBytes(name)
        }
        slotSize = pos
    }

    fun offsets(name: String) = offsets[name]

    private fun lengthInBytes(name: String) =
        when (schema.type(name)) {
            FieldType.INTEGER -> Int.SIZE_BYTES
            FieldType.VARCHAR -> Page.maxLength(schema.length(name))
        }
}