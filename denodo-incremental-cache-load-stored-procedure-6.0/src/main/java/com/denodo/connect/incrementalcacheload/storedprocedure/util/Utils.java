package com.denodo.connect.incrementalcacheload.storedprocedure.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.denodo.vdb.engine.storedprocedure.DatabaseEnvironment;
import com.denodo.vdb.engine.storedprocedure.DatabaseEnvironmentImpl;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureException;

public class Utils {

    private static final Logger logger = Logger.getLogger(Utils.class);

    private static String LAST_CACHE_REFRESH = "@LASTCACHEREFRESH";
    private static int CACHE_MODE_FULL = 3;

    private static DatabaseEnvironment environment;
    private static DatabaseEnvironmentImpl databaseEnvironmentImpl;

    /**
     * Validates if the input parameters are correct and if the cache is enabled in both server and view
     *
     * @param env
     * @param databaseEnvImpl
     * @param inputValues
     * @throws StoredProcedureException
     * @throws SQLException
     */
    public static void validateInputParametersAndCache(DatabaseEnvironment env, DatabaseEnvironmentImpl databaseEnvImpl, Object[] inputValues)
            throws StoredProcedureException, SQLException {

        environment = env;
        databaseEnvironmentImpl = databaseEnvImpl;

        List<String> errorMessages = new LinkedList<>();
        errorMessages.add("\n");

        String databaseName = (String) inputValues[0];
        String viewName = (String) inputValues[1];
        String lastUpdateCondition = (String) inputValues[2];
        String numElementsInClause = (String) inputValues[3];

        boolean validDB = testDatabaseName(databaseName, errorMessages);
        boolean validView = testViewName(viewName, databaseName, validDB, errorMessages);

        // If cache is not valid, the process stops as it might be needed if @LASTCACHEREFRESH parameter is used
        if (validDB && validView) {
            testValidCache(databaseEnvironmentImpl, databaseName, viewName, errorMessages);
        }

        boolean validLastUpdateCondition = testLastUpdateCondition(databaseName, viewName, lastUpdateCondition, validDB, validView,
                errorMessages, inputValues);
        boolean validNumElementsInClause = testValidNumElementsInClause(numElementsInClause, errorMessages);

        // If there are errors, there will be sent to VDP
        if (!validDB || !validView || !validLastUpdateCondition || !validNumElementsInClause) {
            throw new StoredProcedureException(join(errorMessages, "\n"));
        }

    }

    public static List<String> getPkFieldsByViewNameAndDb(DatabaseEnvironment environment, String databaseName, String viewName)
            throws StoredProcedureException {

        List<String> pkFields = new LinkedList<>();
        String viewNameQuotesCleared = viewName.replace("\"", "");

        StringBuilder query = new StringBuilder();
        query.append(" SELECT distinct pk.column_name ");
        query.append(" FROM GET_PRIMARY_KEYS() pk ");
        query.append(" WHERE pk.input_database_name = '").append(databaseName).append("'");
        query.append(" AND pk.input_view_name = '").append(viewNameQuotesCleared).append("'");

        ResultSet rs = null;
        try {
            logger.debug("getPkFieldsByViewNameAndDb(): " + query);
            rs = environment.executeQuery(query.toString());

            while (rs.next()) {
                // We add the quotes to preserve the case of each PK
                pkFields.add("\"" + rs.getString(1) + "\"");
            }
        } catch (SQLException e) {
            logger.debug("ERROR in getPkFieldsByViewNameAndDb(): ", e);
            throw new StoredProcedureException("ERROR getting view PK" + e.getMessage());
        } finally {
            // Close resources
            DBUtils.closeRs(rs);
        }

        if (pkFields == null || pkFields.isEmpty()) {
            throw new StoredProcedureException(
                    "The view " + viewName + " from the DB " + databaseName + " has no primary key. Cache won't be updated");
        }

        logger.debug("pk fields: " + pkFields.toString());
        return pkFields;
    }

    private static String getLastModifiedViewDate(DatabaseEnvironmentImpl databaseEnvironmentImpl, String databaseName, String viewName)
            throws StoredProcedureException {

        // Views and dbs in vdb_cache_querypattern are stored without quotes, so they have to be removed in the query
        String databaseNameQuotesCleared = databaseName.replace("\"", "");
        String viewNameQuotesCleared = viewName.replace("\"", "");

        Connection cacheConnection = databaseEnvironmentImpl.getCacheConnection(databaseNameQuotesCleared, true);

        PreparedStatement ps = null;
        ResultSet rs = null;
        String dateString = null;

        try {
            // This needs to be standard SQL as it needs to work in every database that could be configured as cache
            ps = cacheConnection
                    .prepareStatement("SELECT expirationdate FROM vdb_cache_querypattern WHERE databasename = ? AND viewname = ? ");
            ps.setString(1, databaseNameQuotesCleared);
            ps.setString(2, viewNameQuotesCleared);

            rs = ps.executeQuery();
            if (rs.next()) {
                dateString = DateUtils.millisecondsToStringDate(rs.getLong(1));
            }
        } catch (SQLException e) {
            logger.debug("ERROR in getLastModifiedViewDate(): ", e);
            throw new StoredProcedureException("ERROR getting last modified view date" + e.getMessage());
        } finally {
            // Close resources
            DBUtils.closeRs(rs);
            DBUtils.closePs(ps);
            DBUtils.closeConn(cacheConnection);
        }

        logger.debug("getLastModifiedViewDate(): dateString = " + dateString);
        return dateString;
    }

    private static boolean testDatabaseName(String databaseName, List<String> errorMessages) throws SQLException {

        // Test if databaseName is valid
        boolean validDB = true; // Used to know if it's necessary testing the view / lastUpdateCondition

        if (databaseName == null || databaseName.length() == 0) {
            validDB = false;
            errorMessages.add("database_name can't be empty.");
        } else {
            ResultSet rs = null;
            try {
                // CATALOG_VDP_METADATA_VIEWS is case sensitive and does not work like a select
                // statement. So if the viewName comes rounded by double quotes ("") they must
                // be removed
                String databaseNameQuotesCleared = databaseName.replace("\"", "");
                String query = "SELECT input_database_name FROM CATALOG_VDP_METADATA_VIEWS('"
                        + databaseNameQuotesCleared + "', null) LIMIT 1";
                logger.debug("testDatabaseName(): " + query);
                rs = environment.executeQuery(query);
                // The query returns no rows if there is no database with the provided name
                if (!rs.next()) {
                    throw new StoredProcedureException();
                }
            } catch (StoredProcedureException e) {
                validDB = false;
                errorMessages.add("database_name = '" + databaseName + "' is not valid. ");
                logger.debug("ERROR testDatabaseName() ", e);
            } finally {
                DBUtils.closeRs(rs);
            }
        }

        return validDB;
    }

    private static boolean testViewName(String viewName, String databaseName, boolean validDB, List<String> errorMessages)
            throws SQLException {

        // Test if viewName is valid
        boolean validView = true;
        if (viewName == null || viewName.length() == 0) {
            validView = false;
            errorMessages.add("view_name can't be empty.");
        } else if (validDB) {
            ResultSet rs = null;
            try {
                // CATALOG_VDP_METADATA_VIEWS is case sensitive and does not work like a select
                // statement. So if the viewName comes rounded by double quotes ("") they must
                // be removed
                String databaseNameQuotesCleared = databaseName.replace("\"", "");
                String viewNameQuotesCleared = viewName.replace("\"", "");
                String query = "SELECT input_database_name FROM CATALOG_VDP_METADATA_VIEWS('"
                        + databaseNameQuotesCleared + "', '" + viewNameQuotesCleared + "') LIMIT 1";
                logger.debug("testViewName(): " + query);
                rs = environment.executeQuery(query);
                // The query returns no rows if there is no view in the database with the
                // provided parameters
                if (!rs.next()) {
                    throw new StoredProcedureException();
                }
            } catch (StoredProcedureException e) {
                validView = false;
                errorMessages.add("view_name = '" + viewName + "' does not exists in '" + databaseName + "' database. ");
                logger.debug("ERROR testViewName() ", e);
            } finally {
                DBUtils.closeRs(rs);
            }
        }

        return validView;
    }

    private static boolean testValidCache(DatabaseEnvironmentImpl databaseEnvironmentImpl, String databaseName,
                                          String viewName, List<String> errorMessages)
            throws StoredProcedureException {

        boolean validCache = true;

        boolean isCacheServerEnabled = databaseEnvironmentImpl.isCacheEnabled(
                databaseName.replaceAll("\"", ""));

        if (!isCacheServerEnabled) {
            validCache = false;
            throw new StoredProcedureException("The cache is not enabled in the Server.");
        }

        boolean isViewCacheFull= isViewCacheEnabledFull(databaseName, viewName);

        if (!isViewCacheFull) {
            validCache = false;
            throw new StoredProcedureException("Cache full is not enabled in the view. Please, enable it and try again.");
        }

        return validCache;
    }



    private static boolean testLastUpdateCondition(String databaseName, String viewName, String lastUpdateCondition, boolean validDB,
            boolean validView, List<String> errorMessages, Object[] inputValues) throws SQLException {

        // Test if lastUpdateCondition is valid
        boolean validLastUpdateCondition = true;
        if (lastUpdateCondition == null || lastUpdateCondition.length() == 0) {
            validLastUpdateCondition = false;
            errorMessages.add("last_update_condition can't be empty.");
        } else if (validDB && validView) {

            ResultSet rs = null;
            try {
                // Special case: @LASTCACHEREFRESH
                if (lastUpdateCondition.contains(LAST_CACHE_REFRESH)) {
                    // To update tuples since last cache refresh, we check the max value of
                    // modified date in the cache itself
                    String stringDate = getLastModifiedViewDate(databaseEnvironmentImpl, databaseName, viewName);
                    lastUpdateCondition = lastUpdateCondition.replaceAll(LAST_CACHE_REFRESH, "'" + stringDate + "'");

                    // lastUpdateCondition parameter override
                    inputValues[2] = lastUpdateCondition;

                }

                String query = "select 1 from " + databaseName + "." + viewName + " where " + lastUpdateCondition
                        + " fetch first 1 rows only CONTEXT ('cache' = 'on')";
                logger.debug("testLastUpdateCondition(): " + query);
                rs = environment.executeQuery(query);

            } catch (StoredProcedureException e) {
                validLastUpdateCondition = false;
                errorMessages.add("last_update_condition = '" + lastUpdateCondition + "' is not valid. Alternatively, " +
                        "if you are calling this stored procedure on a view/database with an unicode-based name, please " +
                        "check that you have specified its name surrounded with double-quotes." + e.getMessage());
                logger.debug("ERROR testLastUpdateCondition() ", e);
            } finally {
                DBUtils.closeRs(rs);
            }
        }

        return validLastUpdateCondition;
    }

    private static boolean testValidNumElementsInClause(String numElementsInClauseString, List<String> errorMessages) {

        // Test if numElementsInClause is valid
        boolean validNumElementsInClause = true;
        if (numElementsInClauseString != null) {
            Integer numElementsInClause;
            try {
                numElementsInClause = Integer.valueOf(numElementsInClauseString);
                if (numElementsInClause.intValue() <= 0) {
                    validNumElementsInClause = false;
                    errorMessages.add("num_elements_in_clause must be greater than 0.");
                }
            } catch (Exception e) {
                validNumElementsInClause = false;
                errorMessages.add("num_elements_in_clause = " + numElementsInClauseString + " is not valid.");
            }
        } else {
            validNumElementsInClause = false;
            errorMessages.add("num_elements_in_clause can't be empty.");
        }

        return validNumElementsInClause;
    }

    private static boolean isViewCacheEnabledFull(String databaseName, String viewName) throws StoredProcedureException {

        boolean isCacheFull = false;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            String databaseNameQuotesCleared = databaseName.replace("\"", "");
            String viewNameQuotesCleared = viewName.replace("\"", "");
            String[] params = new String[]{databaseNameQuotesCleared, viewNameQuotesCleared};
            // This needs to be standard SQL as it needs to work in every database that could be configured as cache
            String query = "select cache_status from GET_VIEWS() where input_database_name = ? and input_name = ? ";
            logger.debug("isViewCacheEnabledFull: " + query);
            rs = environment.executeQuery(query, params);

            if (rs.next()) {
                int cacheStatus = rs.getInt(1);
                if (cacheStatus == CACHE_MODE_FULL) {
                    isCacheFull = true;
                }
            }
        } catch (StoredProcedureException | SQLException e) {
            logger.debug("ERROR isViewCacheEnabledFull() ", e);
            throw new StoredProcedureException("ERROR isViewCacheEnabledFull() " + e.getMessage());
        } finally {
            DBUtils.closeRs(rs);
            DBUtils.closePs(ps);
        }

        return isCacheFull;
    }

    /**
     * Joins the elements of the list separated by the separator parameter
     *
     * @param list
     * @param separator
     * @return
     */
    public static String join(List<String> list, String separator) {
        StringBuilder ret = new StringBuilder();
        for (String string: list) {
            ret.append(separator).append(string);
        }
        // Remove separator from the beginning of the string
        return ret.toString().substring(separator.length(), ret.length());
    }
}
