package simpledb.file

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class Page {
    private var bb: ByteBuffer
    private val charset = StandardCharsets.US_ASCII

    constructor(blockSize: Int) {
        bb = ByteBuffer.allocateDirect(blockSize)
    }

    constructor(b: ByteArray) {
        bb = ByteBuffer.wrap(b)
    }

    fun getInt(offset: Int): Int {
        return bb.getInt(offset)
    }

    fun setInt(offset: Int, n: Int) {
        bb.putInt(offset, n)
    }

    fun getBytes(offset: Int): ByteArray {
        bb.position(offset)
        val length = bb.int
        val b = ByteArray(length)
        bb.get(b)
        return b
    }

    fun setBytes(offset: Int, b: ByteArray) {
        bb.position(offset)
        bb.putInt(b.size)
        bb.put(b)
    }

    fun getString(offset: Int): String {
        val b = getBytes(offset)
        return String(b, charset)
    }

    fun setString(offset: Int, s: String) {
        val b = s.toByteArray(charset)
        setBytes(offset, b)
    }

    companion object {
        private val charset = StandardCharsets.US_ASCII
        fun maxLength(strSize: Int): Int {
            val bytesPerChar = charset.newEncoder().maxBytesPerChar()
            return Integer.BYTES + (strSize * (bytesPerChar.toInt()))
        }
    }

    fun contents(): ByteBuffer {
        bb.position(0)
        return bb
    }
}