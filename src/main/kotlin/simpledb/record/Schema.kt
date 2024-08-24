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

    fun addAll(schema: Schema) {
        for (fieldName in schema.fields) {
            add(fieldName, schema)
        }
    }

    fun length(name: String) = info[name]!!.length

    fun type(name: String) = info[name]!!.type

    fun hasField(name: String) = name in fields

    data class FieldInfo(val type: FieldType, val length: Int)
}

enum class FieldType(val number:Int) {
    INTEGER(0), VARCHAR(1);
    companion object {
        fun fieldTypeFactory(number: Int): FieldType =
            when (number) {
                0 -> FieldType.INTEGER
                else -> FieldType.VARCHAR
            }
    }

}

