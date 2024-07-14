package simpledb.record

class Schema {
    val fields: MutableList<String> = mutableListOf()
    private val info: MutableMap<String, FieldInfo> = mutableMapOf()

    fun addField(fieldName: String, type: FieldType, length: Int) {
        fields.add(fieldName)
        info[fieldName] = FieldInfo(type, length)
    }

    fun addIntField(name: String) {
        addField(name, FieldType.INTEGER, 0)
    }

    fun addStringField(name: String, length: Int) {
        addField(name, FieldType.VARCHAR, length)
    }

    fun add(name: String, schema: Schema) {
        addField(name, schema.type(name), schema.length(name))
    }

    fun length(name: String) = info[name]!!.length

    fun type(name: String) = info[name]!!.type

    fun hasField(name: String) = name in fields

    data class FieldInfo(val type: FieldType, val length: Int)
}

enum class FieldType { INTEGER, VARCHAR }