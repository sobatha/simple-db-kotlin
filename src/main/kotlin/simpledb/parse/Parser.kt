package simpledb.parse

import simpledb.query.*
import simpledb.record.*

class Parser(s: String) {
    private val lex = Lexer(s)

    // Methods for parsing predicates, terms, expressions, constants, and fields

    fun field(): String = lex.eatId()

    fun constant(): Constant {
//        println("string: ${lex.matchStringConstant()} , int: ${lex.matchIntConstant()}")
        return if (lex.matchStringConstant()) Constant(lex.eatStringConstant())
        else Constant(lex.eatIntConstant())
    }

    fun expression(): Expression =
        if (lex.matchId()) Expression(field())
        else Expression(constant())

    fun term(): Term {
        val lhs = expression()
        lex.eatDelimiter('=')
        val rhs = expression()
        return Term(lhs, rhs)
    }

    fun predicate(): Predicate {
        val pred = Predicate(term())
        if (lex.matchKeyword("and")) {
            lex.eatKeyword("and")
            pred.conjoinWith(predicate())
        }
        return pred
    }

    // Methods for parsing queries

    fun query(): QueryData {
        lex.eatKeyword("select")
        val fields = selectList()
        lex.eatKeyword("from")
        val tables = tableList()
        var pred = Predicate()
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where")
            pred = predicate()
        }
        return QueryData(fields, tables, pred)
    }

    private fun selectList(): List<String> {
        val L = mutableListOf(field())
        if (lex.matchDelim(',')) {
            lex.eatDelimiter(',')
            L.addAll(selectList())
        }
        return L
    }

    private fun tableList(): Collection<String> {
        val L = mutableListOf(lex.eatId())
        if (lex.matchDelim(',')) {
            lex.eatDelimiter(',')
            L.addAll(tableList())
        }
        return L
    }

    // Methods for parsing the various update commands

    fun updateCmd(): Any = when {
        lex.matchKeyword("insert") -> insert()
        lex.matchKeyword("delete") -> delete()
        lex.matchKeyword("update") -> modify()
        else -> create()
    }

    private fun create(): Any {
        lex.eatKeyword("create")
        return when {
            lex.matchKeyword("table") -> createTable()
            lex.matchKeyword("view") -> createView()
            else -> createIndex()
        }
    }

    // Method for parsing delete commands

    fun delete(): DeleteData {
        lex.eatKeyword("delete")
        lex.eatKeyword("from")
        val tblname = lex.eatId()
        var pred = Predicate()
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where")
            pred = predicate()
        }
        return DeleteData(tblname, pred)
    }

    // Methods for parsing insert commands

    fun insert(): InsertData {
        lex.eatKeyword("insert")
        lex.eatKeyword("into")
        val tblname = lex.eatId()
        lex.eatDelimiter('(')
        val flds = fieldList()
        lex.eatDelimiter(')')
        lex.eatKeyword("values")
        lex.eatDelimiter('(')
        val vals = constList()
        lex.eatDelimiter(')')
        return InsertData(tblname, flds, vals)
    }

    private fun fieldList(): List<String> {
        val L = mutableListOf<String>()
        L.add(field())
        if (lex.matchDelim(',')) {
            lex.eatDelimiter(',')
            L.addAll(fieldList())
        }
//        println("field list: $L")
        return L
    }

    private fun constList(): List<Constant> {
        val L = mutableListOf<Constant>()
        L.add(constant())
        if (lex.matchDelim(',')) {
            lex.eatDelimiter(',')
            L.addAll(constList())
        }
//        println("const list: $L")
        return L
    }

    // Method for parsing modify commands

    fun modify(): ModifyData {
        lex.eatKeyword("update")
        val tblname = lex.eatId()
        lex.eatKeyword("set")
        val fldname = field()
        lex.eatDelimiter('=')
        val newval = expression()
        var pred = Predicate()
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where")
            pred = predicate()
        }
        return ModifyData(tblname, fldname, newval, pred)
    }

    // Method for parsing create table commands

    fun createTable(): CreateTableData {
        lex.eatKeyword("table")
        val tblname = lex.eatId()
        lex.eatDelimiter('(')
        val sch = fieldDefs()
        lex.eatDelimiter(')')
        return CreateTableData(tblname, sch)
    }

    private fun fieldDefs(): Schema {
        val schema = fieldDef()
        if (lex.matchDelim(',')) {
            lex.eatDelimiter(',')
            val schema2 = fieldDefs()
            schema.addAll(schema2)
        }
        return schema
    }

    private fun fieldDef(): Schema {
        val fldname = field()
        return fieldType(fldname)
    }

    private fun fieldType(fldname: String): Schema {
        val schema = Schema()
        if (lex.matchKeyword("int")) {
            lex.eatKeyword("int")
            schema.addIntField(fldname)
        } else {
            lex.eatKeyword("varchar")
            lex.eatDelimiter('(')
            val strLen = lex.eatIntConstant()
            lex.eatDelimiter(')')
            schema.addStringField(fldname, strLen)
        }
        return schema
    }

    // Method for parsing create view commands

    fun createView(): CreateViewData {
        lex.eatKeyword("view")
        val viewname = lex.eatId()
        lex.eatKeyword("as")
        val qd = query()
        return CreateViewData(viewname, qd)
    }

    // Method for parsing create index commands

    fun createIndex(): CreateIndexData {
        lex.eatKeyword("index")
        val idxname = lex.eatId()
        lex.eatKeyword("on")
        val tblname = lex.eatId()
        lex.eatDelimiter('(')
        val fldname = field()
        lex.eatDelimiter(')')
        return CreateIndexData(idxname, tblname, fldname)
    }
}