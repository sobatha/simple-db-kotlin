package simpledb.query

import simpledb.query.Constant
import simpledb.record.RID


/**
 * The interface implemented by all updateable scans.
 */
interface UpdateScan : Scan {
    /**
     * Modify the field value of the current record.
     * @param fldname the name of the field
     * @param val the new value, expressed as a Constant
     */
    fun setVal(fldname: String, value: Constant)

    /**
     * Modify the field value of the current record.
     * @param fldname the name of the field
     * @param val the new integer value
     */
    fun setInt(fldname: String, value: Int)

    /**
     * Modify the field value of the current record.
     * @param fldname the name of the field
     * @param val the new string value
     */
    fun setString(fldname: String, value: String)

    /**
     * Insert a new record somewhere in the scan.
     */
    fun insert()

    /**
     * Delete the current record from the scan.
     */
    fun delete()

    /**
     * Return the id of the current record.
     * @return the id of the current record
     */
    fun getRid(): RID

    /**
     * Position the scan so that the current record has
     * the specified id.
     * @param rid the id of the desired record
     */
    fun moveToRid(rid: RID)
}
