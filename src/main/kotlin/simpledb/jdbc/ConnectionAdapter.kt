package simpledb.jdbc

import java.sql.*
import java.util.*
import java.util.concurrent.Executor

interface ConnectionAdapter: Connection {
    override fun clearWarnings() {
        throw SQLException("operation not implemented")
    }

    override fun close() {
        throw SQLException("operation not implemented")
    }

    override fun commit() {
        throw SQLException("operation not implemented")
    }

    override fun createArrayOf(typeName: String?, elements: Array<out Any>?): java.sql.Array {
        throw SQLException("operation not implemented")
    }

    override fun createBlob(): Blob {
        throw SQLException("operation not implemented")
    }

    override fun createClob(): Clob {
        throw SQLException("operation not implemented")
    }

    override fun createNClob(): NClob {
        throw SQLException("operation not implemented")
    }

    override fun createSQLXML(): SQLXML {
        throw SQLException("operation not implemented")
    }

    override fun createStatement(): Statement {
        throw SQLException("operation not implemented")
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement {
        throw SQLException("operation not implemented")
    }

    override fun createStruct(typeName: String?, attributes: Array<out Any>?): Struct {
        throw SQLException("operation not implemented")
    }

    override fun getAutoCommit(): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun getCatalog(): String {
        throw SQLException("operation not implemented")
    }

    override fun getClientInfo(): Properties {
        throw SQLException("operation not implemented")
    }

    override fun getClientInfo(name: String?): String {
        throw SQLException("operation not implemented")
    }

    override fun getHoldability(): Int {
        throw SQLException("operation not implemented")
    }

    override fun getMetaData(): DatabaseMetaData {
        throw SQLException("operation not implemented")
    }

    override fun getTransactionIsolation(): Int {
        throw SQLException("operation not implemented")
    }

    override fun getTypeMap(): MutableMap<String, Class<*>> {
        throw SQLException("operation not implemented")
    }

    override fun getWarnings(): SQLWarning {
        throw SQLException("operation not implemented")
    }

    override fun isClosed(): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun isReadOnly(): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun isValid(timeout: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun nativeSQL(sql: String?): String {
        throw SQLException("operation not implemented")
    }

    override fun prepareCall(sql: String?): CallableStatement {
        throw SQLException("operation not implemented")
    }

    override fun prepareCall(sql: String?, resultSetType: Int, resultSetConcurrency: Int): CallableStatement {
        throw SQLException("operation not implemented")
    }

    override fun prepareCall(
        sql: String?,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): CallableStatement {
        throw SQLException("operation not implemented")
    }

    override fun prepareStatement(sql: String?): PreparedStatement {
        throw SQLException("operation not implemented")
    }

    override fun prepareStatement(sql: String?, autoGeneratedKeys: Int): PreparedStatement {
        throw SQLException("operation not implemented")
    }

    override fun prepareStatement(sql: String?, columnIndexes: IntArray?): PreparedStatement {
        throw SQLException("operation not implemented")
    }

    override fun prepareStatement(sql: String?, columnNames: Array<out String>?): PreparedStatement {
        throw SQLException("operation not implemented")
    }

    override fun prepareStatement(sql: String?, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement {
        throw SQLException("operation not implemented")
    }

    override fun prepareStatement(
        sql: String?,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): PreparedStatement {
        throw SQLException("operation not implemented")
    }

    override fun releaseSavepoint(savepoint: Savepoint?) {
        throw SQLException("operation not implemented")
    }

    override fun rollback() {
        throw SQLException("operation not implemented")
    }

    override fun rollback(savepoint: Savepoint?) {
        throw SQLException("operation not implemented")
    }

    override fun setAutoCommit(autoCommit: Boolean) {
        throw SQLException("operation not implemented")
    }

    override fun setCatalog(catalog: String?) {
        throw SQLException("operation not implemented")
    }

    override fun setClientInfo(properties: Properties?) {
        throw SQLException("operation not implemented")
    }

    override fun setClientInfo(name: String?, value: String?) {
        throw SQLException("operation not implemented")
    }

    override fun setHoldability(holdability: Int) {
        throw SQLException("operation not implemented")
    }

    override fun setReadOnly(readOnly: Boolean) {
        throw SQLException("operation not implemented")
    }

    override fun setSavepoint(name: String?): Savepoint {
        throw SQLException("operation not implemented")
    }

    override fun setSavepoint(): Savepoint {
        throw SQLException("operation not implemented")
    }

    override fun setTransactionIsolation(level: Int) {
        throw SQLException("operation not implemented")
    }

    override fun setTypeMap(map: MutableMap<String, Class<*>>?) {
        throw SQLException("operation not implemented")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        throw SQLException("operation not implemented")
    }

    override fun abort(executor: Executor?) {
        throw SQLException("operation not implemented")
    }

    override fun getNetworkTimeout(): Int {
        throw SQLException("operation not implemented")
    }

    override fun getSchema(): String {
        throw SQLException("operation not implemented")
    }

    override fun setNetworkTimeout(executor: Executor?, milliseconds: Int) {
        throw SQLException("operation not implemented")
    }

    override fun setSchema(schema: String?) {
        throw SQLException("operation not implemented")
    }
}