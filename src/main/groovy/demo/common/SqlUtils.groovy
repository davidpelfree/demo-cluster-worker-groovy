package demo.common

import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.sql.DriverManager

/**
 * Contains all SQL related stuff.
 *
 * Written in plain SQL over JDBC.
 *
 * Implementing the design approaches:
 *   1. Do not use redundant frameworks if you don't must.
 *   2. See the logic in your eyes with clear code, no AOP, no too much dynamics.
 *   3. SQL is something you care about, it controls your performance and security, so don't hide it!
 *
 * Here, we're Postgres specific.
 */
@CompileStatic
class SqlUtils implements Closeable {

    static final Logger logger = LoggerFactory.getLogger(SqlUtils.class)

    static final String TABLE_NAME = 'processing'


    final Connection connection

    SqlUtils(String host, String port, String user, String password, String dbName) {
        Class.forName("org.postgresql.Driver")
        connection = DriverManager.getConnection("jdbc:postgresql://$host:$port/$dbName", user, password)
        connection.autoCommit = true
    }

    /**
     * We're running a container in a multi-process environment.
     * Our shared, common, central "state" and "locking" infrastructure is the database.
     * So each process / container takes a small batch of tasks to work on.
     *
     * You detect "not-yet-processed" rows by checking for their status: the 'selectByStatus' argument.
     *
     * When you assign some rows to process, you change their status to 'newStatusToUpdate' argument.
     *
     * @param batchSize how many rows to mark as processed by this processor (= a single container in a multi-container cluster)
     * @param newStatusToUpdate e.g. 'fetched-for-processing'
     * @param selectByStatus e.g. 'waiting-for-processing', or usually just null
     * @return list of database row IDs to process
     */
    List<String> getRowsForProcessing(int batchSize, String newStatusToUpdate, String selectByStatus = null) {
        final result = []

        // Do "bulk select" by first updating status of some available rows, then return them.
        // This is done for scaling reasons:
        //   1. process a small batch every time without memory blowing
        //   2. allow parallel processing by multiple threads / processes
        final ps = connection.prepareStatement(
                "update $TABLE_NAME " +
                        "set status=?, timestamp = now() where id in (" +
                        "select id from $TABLE_NAME " +
                        "where status ${selectByStatus == null ? 'is null' : '=?'} limit ?" +
                        ") returning id;"
        )
        int i = 0
        ps.setString(++i, newStatusToUpdate)
        if (selectByStatus != null) ps.setString(++i, selectByStatus)
        ps.setInt(++i, batchSize)
        final rs = ps.executeQuery()
        while (rs.next()) {
            result.add(rs.getString(1))
        }
        result
    }

    int updateStatusWithThrowable(String rowKey, String status, String failureReason = null, Throwable throwable = null) {
        updateStatus(rowKey, status, failureReason, throwableToString(throwable))
    }

    int updateStatus(String rowKey, String status, String failureReason = null, String detailedReason = null) {
        final ps = connection.prepareStatement("update $TABLE_NAME set " +
                "timestamp = now(), status = ?, failure_reason = ?, failure_exception = ? " +
                "where id = ?;")


        if (failureReason && failureReason.length() > 99) {
            failureReason = failureReason[0..99]
        }

        int i = 0
        ps.setString(++i, status)
        ps.setString(++i, failureReason)
        ps.setString(++i, detailedReason)
        ps.setString(++i, rowKey)
        final result = ps.executeUpdate()
        result
    }


    @Override
    void close() throws IOException {
        connection?.close()
    }

    static String throwableToString(Throwable t) {
        if (t != null) {
            def swriter = new StringWriter()
            final writer = new PrintWriter(swriter)
            t.printStackTrace(writer)
            writer.close()
            return swriter.toString()
        }
        return null
    }

}

