package org.example.simpledb.file

/*
 * identifies a specific block by irs file name and logical block number
 */
class BlockId(private val fileName: String, private val number: Int) {
    override fun toString(): String {
        return "[file $fileName, block $number]"
    }
    override fun hashCode(): Int {
        return toString().hashCode()
    }
}