package simpledb.parse

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe


import java.util.Scanner

// Will successfully read in lines of text denoting an
// SQL expression of the form "id = c" or "c = id".

fun main() {
    val sc = Scanner(System.`in`)
    while (sc.hasNext()) {
        val s = sc.nextLine()
        val lex = Lexer(s)
        val (x, y) = if (lex.matchId()) {
            val id = lex.eatId()
            lex.eatDelimiter('=')
            val constant = lex.eatIntConstant()
            Pair(id, constant)
        } else {
            val constant = lex.eatIntConstant()
            lex.eatDelimiter('=')
            val id = lex.eatId()
            Pair(id, constant)
        }
        println("$x equals $y")
    }
    sc.close()
}
