package com.denodo.connect.incrementalcacheload.storedprocedure.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.denodo.vdb.engine.storedprocedure.DatabaseEnvironment;
import com.denodo.vdb.engine.storedprocedure.DatabaseEnvironmentImpl;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureException;

public class Utils {

    private static String LAST_CACHE_REFRESH = "@LASTCACHEREFRESH";

    private static DatabaseEnvironment environment;
    private static DatabaseEnvironmentImpl databaseEnvironmentImpl;

    // TODO: Improve strategy and avoid cyclomatic complexity
    public static void validateInputParameters(DatabaseEnvironment env, DatabaseEnvironmentImpl databaseEnvImpl, Object[] inputValues)
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
        boolean validLastUpdateCondition = testLastUpdateCondition(databaseName, viewName, lastUpdateCondition, validDB, validView,
                errorMessages, inputValues);
        boolean validNumElementsInClause = testValidNumElementsInClause(numElementsInClause, errorMessages);

        // If there are errors, there will be sent to VDP
        if (!validDB || !validView || !validLastUpdateCondition || !validNumElementsInClause) {
            throw new StoredProcedureException(StringUtils.join(errorMessages, "\n"));
        }

    }

    public static List<String> getPkFieldsByViewNameAndDb(DatabaseEnvironment environment, String databaseName, String viewName)
            throws StoredProcedureException, SQLException {

        List<String> pkFields = new LinkedList<>();

        // FIXME OJO INVOCANDO DESDE OTRA DB
        StringBuilder query = new StringBuilder();
        query.append(" SELECT distinct pk.column_name ");
        query.append(" FROM GET_PRIMARY_KEYS() pk ");
        query.append(" WHERE pk.input_database_name = '").append(databaseName).append("'");
        query.append(" AND pk.input_view_name = '").append(viewName).append("'");
        ResultSet rs = environment.executeQuery(query.toString());

        while (rs.next()) {
            pkFields.add(rs.getString(1));
        }

        rs.close();

        return pkFields;
    }

    private static String getLastModifiedViewDate(DatabaseEnvironmentImpl databaseEnvironmentImpl, String databaseName, String viewName)
            throws StoredProcedureException, SQLException {

        Connection cacheConnection = databaseEnvironmentImpl.getCacheConnection(databaseName, true);

        PreparedStatement ps = cacheConnection
                .prepareStatement("SELECT expirationdate FROM vdb_cache_querypattern WHERE databasename = ? AND viewname = ? ");
        ps.setString(1, databaseName);
        ps.setString(2, viewName);

        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            return DateUtils.millisecondsToStringDate(rs.getLong(1));
        }

        return null;
    }

    private static boolean testDatabaseName(String databaseName, List<String> errorMessages) throws SQLException {

        // Test if databaseName is valid
        boolean validDB = true; // Used to know if it's necessary testing the view / lastUpdateCondition

        if (StringUtils.isEmpty(databaseName)) {
            validDB = false;
            errorMessages.add("database_name can't be empty.");
        } else {
            ResultSet rs = null;
            try {
                rs = environment
                        .executeQuery("SELECT input_database_name FROM CATALOG_VDP_METADATA_VIEWS('" + databaseName + "', null) LIMIT 1");
                // The query returns no rows if there is no database with the provided name
                if (!rs.next()) {
                    throw new StoredProcedureException();
                }
            } catch (StoredProcedureException e) {
                validDB = false;
                errorMessages.add("database_name = '" + databaseName + "' is not valid.");
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }

        return validDB;
    }

    private static boolean testViewName(String viewName, String databaseName, boolean validDB, List<String> errorMessages)
            throws SQLException {

        // Test if viewName is valid
        boolean validView = true;
        if (StringUtils.isEmpty(viewName)) {
            validView = false;
            errorMessages.add("view_name can't be empty.");
        } else if (validDB) {
            ResultSet rs = null;
            try {
                rs = environment.executeQuery(
                        "SELECT input_database_name FROM CATALOG_VDP_METADATA_VIEWS('" + databaseName + "', '" + viewName + "') LIMIT 1");
                // The query returns no rows if there is no view in the database with the
                // provided parameters
                if (!rs.next()) {
                    throw new StoredProcedureException();
                }
            } catch (StoredProcedureException e) {
                validView = false;
                errorMessages.add("view_name = '" + viewName + "' does not exists in '" + databaseName + "' database.");
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }

        return validView;
    }

    private static boolean testLastUpdateCondition(String databaseName, String viewName, String lastUpdateCondition, boolean validDB,
            boolean validView, List<String> errorMessages, Object[] inputValues) throws SQLException {

        // Test if lastUpdateCondition is valid
        boolean validLastUpdateCondition = true;
        if (StringUtils.isEmpty(lastUpdateCondition)) {
            validLastUpdateCondition = false;
            errorMessages.add("last_update_condition can't be empty.");
        } else if (validDB && validView) {
            try {
                // Special case: @LASTCACHEREFRESH
                if (lastUpdateCondition.contains(LAST_CACHE_REFRESH)) {
                    // To update tuples since last cache refresh, we check the max value of
                    // modified date in the cache itself
                    String stringDate = getLastModifiedViewDate(databaseEnvironmentImpl, databaseName, viewName);
                    lastUpdateCondition = lastUpdateCondition.replaceAll(LAST_CACHE_REFRESH, "'" + stringDate + "'");

                    // lastUpdateCondition parameter override
                    inputValues[2] = lastUpdateCondition;

                    // TODO
                    // HACER PRUEBAS PARA VALIDAR LAS FECHAS
                }

                environment.executeQuery("select 1 from " + databaseName + "." + viewName + " where " + lastUpdateCondition
                        + " fetch first 1 rows only CONTEXT ('cache' = 'on')");

            } catch (StoredProcedureException e) {
                validLastUpdateCondition = false;
                errorMessages.add("last_update_condition = '" + lastUpdateCondition + "' is not valid.");
            }
        }

        return validLastUpdateCondition;
    }

    private static boolean testValidNumElementsInClause(String numElementsInClauseString, List<String> errorMessages) {

        // Test if numElementsInClause is valid
        boolean validNumElementsInClause = true;
        if (StringUtils.isNotBlank(numElementsInClauseString)) {
            Integer numElementsInClause = null;
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

}
