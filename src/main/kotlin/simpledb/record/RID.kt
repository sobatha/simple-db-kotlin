package simpledb.record

data class RID(val blockNumber: Int, val slot: Int) {
    override fun hashCode(): Int {
        var result = blockNumber
        result = 31 * result + slot
        return result
    }

    override fun toString(): String {
        return "[$blockNumber, $slot]"
    }
}