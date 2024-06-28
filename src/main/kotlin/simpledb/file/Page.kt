package simpledb.file

import java.nio.ByteBuffer
import java.nio.charset.Charset

/*
 * holds the contents of a disk block
 * constructor creates a page that gets its memory from an operating system IO buffer.
 * second constructor creates a page that gets its memory from a Java array.
 */
class Page {
    private val DEFAULT_CHARSET: Charset = Charsets.UTF_8
    private var byteBuffer: ByteBuffer

    constructor(blockSize: Int) {
        byteBuffer = ByteBuffer.allocateDirect(blockSize)
    }

    constructor(bytes: ByteArray) : this(0) {
        byteBuffer = ByteBuffer.wrap(bytes)
    }

    fun getInt(offset: Int) = byteBuffer.getInt(offset)

    fun setInt(offset: Int, n: Int) = byteBuffer.putInt(offset, n)

    fun getBytes(offset: Int): ByteArray {
        byteBuffer.position(offset)
        val length = byteBuffer.getInt()
        val b = ByteArray(length)
        byteBuffer.get(b)
        return b
    }

    fun setBytes(offset: Int, b: ByteArray) {
        byteBuffer.position(offset)
        byteBuffer.putInt(b.size)
        byteBuffer.put(b)
    }

    fun getString(offset: Int): String {
        val b = getBytes(offset)
        return String(b, DEFAULT_CHARSET)
    }

    fun setString(offset: Int, string: String) {
        val b = string.toByteArray()
        setBytes(offset, b)
    }

    fun contents(): ByteBuffer {
        byteBuffer.position(0)
        return byteBuffer
    }

    companion object {
        private val CHARSET: Charset = Charsets.UTF_8
        fun maxLength(strLen: Int): Int {
            val bytePerChar = CHARSET.newEncoder().maxBytesPerChar()
            return (Int.SIZE_BYTES + (strLen * bytePerChar)).toInt()
        }
    }
}