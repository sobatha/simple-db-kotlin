package simpledb.file

import java.io.File
import java.io.IOException
import java.io.RandomAccessFile

class FileMgr(private val dbDirectory: File, val blockSize: Int) {
    private val openFiles: MutableMap<String, RandomAccessFile> = mutableMapOf()
    val isNew = !dbDirectory.exists()

    init {
        if (isNew) dbDirectory.mkdirs()

        dbDirectory.list()?.forEach {
            if (it.startsWith("temp")) File(dbDirectory, it).delete()
        }
    }
    @Synchronized
    fun read(blk: BlockId, p: Page) {
        try {
            val f = getFile(blk.fileName)
            f.seek((blk.number * blockSize).toLong())
            f.channel.read(p.contents())
        } catch (e: IOException) {
            throw RuntimeException("cannot read block $blk $e")
        }
    }

    @Synchronized
    fun write(blk: BlockId, p: Page) {
        try {
            val f = getFile(blk.fileName)
            f.seek((blk.number * blockSize).toLong())
            f.channel.write(p.contents())
        } catch (e: IOException) {
            throw RuntimeException("cannot write block $blk")
        }
    }

    @Synchronized
    fun append(fileName: String): BlockId {
        val newBlkNum = length(fileName)
        val blk = BlockId(fileName, newBlkNum)
        val b = ByteArray(blockSize)
        try {
            val f = getFile(blk.fileName)
            f.seek((blk.number * blockSize).toLong())
            f.write(b)
        } catch (e: IOException) {
            throw RuntimeException("cannot append block $blk")
        }
        return blk
    }
    private fun getFile(fileName: String): RandomAccessFile {
        return openFiles[fileName] ?: run {
            val dbTable = File(dbDirectory, fileName)
            val newFile = RandomAccessFile(dbTable, "rws")
            openFiles[fileName] = newFile
            newFile
        }
    }

    fun length(fileName: String): Int {
        try {
            val f = getFile(fileName)
            return (f.length() / blockSize).toInt()
        } catch (e: IOException) {
            throw RuntimeException("cannot access $fileName")
        }
    }
}