package simpledb.query

import simpledb.plan.Plan
import simpledb.record.*
import kotlin.math.max

/**
 * A term is a comparison between two expressions.
 */
class Term(private val leftExpression: Expression, private val rightExpression: Expression) {

    fun isSatisfied(s: Scan): Boolean {
        val leftValue = leftExpression.evaluate(s)
        val rightValue = rightExpression.evaluate(s)
        return rightValue == leftValue
    }

    fun reductionFactor(plan: Plan): Int {
        val leftSideExpressionName: String
        val rightSideExpressionName: String
        if (leftExpression.isFieldName() && rightExpression.isFieldName()) {
            leftSideExpressionName = leftExpression.asFieldName()!!
            rightSideExpressionName = leftExpression.asFieldName()!!
            return plan.distinctValues(leftSideExpressionName)
                .coerceAtLeast(plan.distinctValues(rightSideExpressionName))
        }
        if (leftExpression.isFieldName()) {
            leftSideExpressionName = leftExpression.asFieldName()!!
            return plan.distinctValues(leftSideExpressionName)
        }
        if (rightExpression.isFieldName()) {
            rightSideExpressionName = rightExpression.asFieldName()!!
            return plan.distinctValues(rightSideExpressionName)
        }
        return if (leftExpression.asConstant()!! == rightExpression.asConstant()) {
            1
        } else {
            Integer.MAX_VALUE
        }
    }

    fun equatesWithConstant(fldname: String): Constant? {
        return when {
            leftExpression.isFieldName() && leftExpression.asFieldName() == fldname && !rightExpression.isFieldName() -> rightExpression.asConstant()
            rightExpression.isFieldName() && rightExpression.asFieldName() == fldname && !leftExpression.isFieldName() -> leftExpression.asConstant()
            else -> null
        }
    }

    fun equatesWithField(fldname: String): String? {
        return when {
            leftExpression.isFieldName() && leftExpression.asFieldName() == fldname && rightExpression.isFieldName() -> rightExpression.asFieldName()
            rightExpression.isFieldName() && rightExpression.asFieldName() == fldname && leftExpression.isFieldName() -> leftExpression.asFieldName()
            else -> null
        }
    }
    fun appliesTo(sch: Schema): Boolean = leftExpression.appliesTo(sch) && rightExpression.appliesTo(sch)

    override fun toString(): String = "${leftExpression}=${rightExpression}"
}