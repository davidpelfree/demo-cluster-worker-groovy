package demo

import demo.common.DbStatus
import demo.common.SqlUtils
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Simple 1-class demonstrating the power of Groovy
 * to act as a containerized-worker in a cluster.
 */
@CompileStatic
class Main {

    static Logger logger = LoggerFactory.getLogger(Main.class)

    static String DB_HOST
    static String DB_PORT
    static String DB_USER_NAME
    static String DB_PASSWORD
    static String DB_NAME
    static int BATCH_SIZE

    static void main(String[] args) {

        // ---------------------------------------------------------------------------------------------
        // Configuration parameters
        // ---------------------------------------------------------------------------------------------
        DB_HOST = getEnvironmentVariableOrHalt('DB_HOST')
        DB_PORT = getEnvironmentVariableOrHalt('DB_PORT')
        DB_USER_NAME = getEnvironmentVariableOrHalt('DB_USER_NAME')
        DB_PASSWORD = getEnvironmentVariableOrHalt('DB_PASSWORD')
        DB_NAME = getEnvironmentVariable('DB_NAME') ?: 'public' // default value, if no environment variable supplied
        BATCH_SIZE = (getEnvironmentVariable('BATCH_SIZE') as Integer) ?: 10

        logger.info "Parameters:"
        logger.info "DB_HOST: $DB_HOST"
        logger.info "DB_PORT: $DB_PORT"
        logger.info "DB_USER_NAME: $DB_USER_NAME"
        // Do not expose password of course! :)
        logger.info "DB_NAME: $DB_NAME"
        logger.info "BATCH_SIZE: $BATCH_SIZE"

        // ---------------------------------------------------------------------------------------------
        // Init stuff
        // ---------------------------------------------------------------------------------------------

        final sql = new SqlUtils(DB_HOST, DB_PORT, DB_USER_NAME, DB_PASSWORD, DB_NAME)

        // ---------------------------------------------------------------------------------------------

        sql.withCloseable {
            try {
                while (true) {
                    final rowIdsToProcess = sql.getRowsForProcessing(BATCH_SIZE, DbStatus.fetched.name(), null)
                    if (rowIdsToProcess.size() == 0) {
                        logger.info "No more rows to process found. Exiting."
                        break
                    }
                    logger.info "Processing batch of ${rowIdsToProcess.size()} rows ..."

                    rowIdsToProcess.each {String rowId->
                        try {
                            // actual row processing logic is here.
                        } catch (e) {
                            logger.error("Error while processing row ID '${rowId}': $e", e)
                            sql.updateStatusWithThrowable(rowId, DbStatus.failed.name(), e.toString(), e)
                        }
                    }
                }
            } catch (e) {
                logger.error(e.toString(), e)
            } finally {

            }
        }
    }

    static String getEnvironmentVariable(String key) {
        new ProcessBuilder().environment().get(key)
    }

    static String getEnvironmentVariableOrHalt(String key) {
        final value = getEnvironmentVariable(key)
        if (!value) {
            throw new IllegalArgumentException("Missing environment variable: '$key'")
        }
        value
    }
}
