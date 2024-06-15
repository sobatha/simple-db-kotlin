package simpledb.log
import simpledb.file.Page
import simpledb.server.SimpleDB


fun main() {
    val db = SimpleDB("logtest")
    val log = db.logMgr

    fun printLogRecords(m: String) {
        println(m)
        val iter = log.iterator()
        while (iter.hasNext()) {
            val rec = iter.next()
            val page = Page(rec)
            val message = page.getString(0)
            val npos = Page.maxLength(message.length)
            val num = page.getInt(npos)
            println("got record ${page.getString(0)}, $num")
        }
    }

    fun createLogRecord(s: String, n: Int) : ByteArray {
        val npos = Page.maxLength(s.length)
        val b = ByteArray(npos + Integer.BYTES)
        val page = Page(b)
        page.setString(0, s)
        page.setInt(npos, n)
        return b
    }

    fun createRecords(start: Int, end: Int) {
        println("creating records ")
        for (i in start..end) {
            val rec = createLogRecord("record $i", i)
            val lsn = log.append(rec)
            println("appended record $i with lsn $lsn")
        }
    }

    createRecords(1, 10)
    printLogRecords("first 35 records")
    createRecords(30, 50)
    log.flush(65)
    printLogRecords("records 36-70")
}
