package simpledb.query

import simpledb.record.*

/**
 * The interface corresponding to SQL expressions.
 */
class Expression {
    private var constant: Constant? = null
    private var fldname: String? = null

    constructor(constant: Constant) {
        this.constant = constant
                this.fldname = null
    }

    constructor(fldname: String) {
        this.constant = null
        this.fldname = fldname
    }

    /**
     * Evaluate the expression with respect to the
     * current record of the specified scan.
     * @param s the scan
     * @return the value of the expression, as a Constant
     */
    fun evaluate(s: Scan): Constant = constant ?: s.getVal(fldname!!)

    /**
     * Return true if the expression is a field reference.
     * @return true if the expression denotes a field
     */
    fun isFieldName(): Boolean = fldname != null

    /**
     * Return the constant corresponding to a constant expression,
     * or null if the expression does not
     * denote a constant.
     * @return the expression as a constant
     */
    fun asConstant(): Constant? = constant

    /**
     * Return the field name corresponding to a constant expression,
     * or null if the expression does not
     * denote a field.
     * @return the expression as a field name
     */
    fun asFieldName(): String? = fldname

    /**
     * Determine if all of the fields mentioned in this expression
     * are contained in the specified schema.
     * @param sch the schema
     * @return true if all fields in the expression are in the schema
     */
    fun appliesTo(sch: Schema): Boolean = constant != null || sch.hasField(fldname!!)

    override fun toString(): String = constant?.toString() ?: fldname!!
}