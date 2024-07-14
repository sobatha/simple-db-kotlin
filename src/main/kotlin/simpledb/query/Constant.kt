package simpledb.query

/**
 * The class that denotes values stored in the database.
 */
data class Constant(private val ival: Int? = null, private val sval: String? = null) : Comparable<Constant> {

    constructor(ival: Int) : this(ival, null)
    constructor(sval: String) : this(null, sval)

    fun asInt(): Int? {
        return ival
    }

    fun asString(): String? {
        return sval
    }

    override fun compareTo(other: Constant): Int {
        return when {
            ival != null && other.ival != null -> ival.compareTo(other.ival)
            sval != null && other.sval != null -> sval.compareTo(other.sval)
            else -> throw IllegalArgumentException("Cannot compare constants of different types")
        }
    }

    override fun toString(): String {
        return ival?.toString() ?: sval.toString()
    }
}
