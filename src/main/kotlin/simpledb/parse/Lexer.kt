package simpledb.parse

import java.io.IOException
import java.io.StreamTokenizer
import java.io.StringReader

object BadSyntaxException : Throwable() {

}

class Lexer(s: String) {
    private val keywords: Set<String>
    private val tok: StreamTokenizer

    init {
        keywords = initKeywords()
        tok = StreamTokenizer(StringReader(s)).apply {
            ordinaryChar('.'.code)
            wordChars('_'.code, '_'.code)
            lowerCaseMode(true)
        }
        nextToken()
    }

    // Methods to check the status of the current token

    fun matchDelim(d: Char): Boolean = d.code == tok.ttype

    fun matchIntConstant(): Boolean = tok.ttype == StreamTokenizer.TT_NUMBER

    fun matchStringConstant(): Boolean = '\'' == tok.ttype.toChar()

    fun matchKeyword(w: String): Boolean =
        tok.ttype == StreamTokenizer.TT_WORD && tok.sval == w

    fun matchId(): Boolean =
        tok.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tok.sval)

    // Methods to "eat" the current token

    fun eatDelim(d: Char) {
        if (!matchDelim(d)) throw BadSyntaxException
        nextToken()
    }

    fun eatIntConstant(): Int {
        if (!matchIntConstant()) throw BadSyntaxException
        val i = tok.nval.toInt()
        nextToken()
        return i
    }

    fun eatStringConstant(): String {
        if (!matchStringConstant()) throw BadSyntaxException
        val s = tok.sval // constants are not converted to lower case
        nextToken()
        return s
    }

    fun eatKeyword(w: String) {
        if (!matchKeyword(w)) throw BadSyntaxException
        nextToken()
    }

    fun eatId(): String {
        if (!matchId()) throw BadSyntaxException
        val s = tok.sval
        nextToken()
        return s
    }

    private fun nextToken() {
        try {
            tok.nextToken()
        } catch (e: IOException) {
            throw BadSyntaxException
        }
    }

    private fun initKeywords(): Set<String> = setOf(
        "select", "from", "where", "and",
        "insert", "into", "values", "delete", "update", "set",
        "create", "table", "int", "varchar", "view", "as", "index", "on"
    )
}