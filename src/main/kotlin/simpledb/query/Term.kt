package simpledb.query

import simpledb.plan.Plan
import simpledb.record.*
import kotlin.math.max

/**
 * A term is a comparison between two expressions.
 */
class Term(private val leftExpression: Expression, private val rightExpression: Expression) {

    /**
     * Return true if both of the term's expressions
     * evaluate to the same constant,
     * with respect to the specified scan.
     * @param s the scan
     * @return true if both expressions have the same value in the scan
     */
    fun isSatisfied(s: Scan): Boolean {
        val leftValue = leftExpression.evaluate(s)
        val rightValue = rightExpression.evaluate(s)
        return rightValue == leftValue
    }

    /**
     * Calculate the extent to which selecting on the term reduces
     * the number of records output by a query.
     * For example if the reduction factor is 2, then the
     * term cuts the size of the output in half.
     * @param p the query's plan
     * @return the integer reduction factor.
     */
    fun reductionFactor(p: Plan): Any {
        return when {
            leftExpression.isFieldName() && rightExpression.isFieldName() -> {
                val lhsName = leftExpression.asFieldName()!!
                val rhsName = rightExpression.asFieldName()!!
                TODO()
//                max(p.distinctValues(lhsName), p.distinctValues(rhsName))
            }
            leftExpression.isFieldName() -> p.distinctValues(leftExpression.asFieldName()!!)
            rightExpression.isFieldName() -> p.distinctValues(rightExpression.asFieldName()!!)
            else -> if (leftExpression.asConstant() == rightExpression.asConstant()) 1 else Int.MAX_VALUE
        }
    }

    /**
     * Determine if this term is of the form "F=c"
     * where F is the specified field and c is some constant.
     * If so, the method returns that constant.
     * If not, the method returns null.
     * @param fldname the name of the field
     * @return either the constant or null
     */
    fun equatesWithConstant(fldname: String): Constant? {
        return when {
            leftExpression.isFieldName() && leftExpression.asFieldName() == fldname && !rightExpression.isFieldName() -> rightExpression.asConstant()
            rightExpression.isFieldName() && rightExpression.asFieldName() == fldname && !leftExpression.isFieldName() -> leftExpression.asConstant()
            else -> null
        }
    }

    /**
     * Determine if this term is of the form "F1=F2"
     * where F1 is the specified field and F2 is another field.
     * If so, the method returns the name of that field.
     * If not, the method returns null.
     * @param fldname the name of the field
     * @return either the name of the other field, or null
     */
    fun equatesWithField(fldname: String): String? {
        return when {
            leftExpression.isFieldName() && leftExpression.asFieldName() == fldname && rightExpression.isFieldName() -> rightExpression.asFieldName()
            rightExpression.isFieldName() && rightExpression.asFieldName() == fldname && leftExpression.isFieldName() -> leftExpression.asFieldName()
            else -> null
        }
    }

    /**
     * Return true if both of the term's expressions
     * apply to the specified schema.
     * @param sch the schema
     * @return true if both expressions apply to the schema
     */
    fun appliesTo(sch: Schema): Boolean = leftExpression.appliesTo(sch) && rightExpression.appliesTo(sch)

    override fun toString(): String = "${leftExpression}=${rightExpression}"
}