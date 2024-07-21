package simpledb.metadata

class StatInfo(private val numBlocks: Int, private val numRecs: Int) {

    fun blocksAccessed(): Int {
        return numBlocks
    }

    fun recordsOutput(): Int {
        return numRecs
    }

    fun distinctValues(fldname: String): Int {
        return 1 + (numRecs / 3)
    }
}
