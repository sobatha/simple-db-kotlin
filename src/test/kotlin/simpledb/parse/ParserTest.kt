package simpledb.parse

import java.util.*

object ParserTest {
    @JvmStatic
    fun main(args: Array<String>) {
        val sc = Scanner(System.`in`)
        print("Enter an SQL statement: ")
        while (sc.hasNext()) {
            val s = sc.nextLine()
            val p = Parser(s)
            try {
                if (s.startsWith("select")) p.query()
                else p.updateCmd()
                println("yes")
            } catch (ex: BadSyntaxException) {
                println("no")
            }
            print("Enter an SQL statement: ")
        }
        sc.close()
    }
}