package com.denodo.connect.incrementalcacheload.storedprocedure.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.denodo.vdb.engine.storedprocedure.DatabaseEnvironment;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureException;

public class Utils {

    // TODO: Improve strategy and avoid cyclomatic complexity
    public static void validateInputParameters(DatabaseEnvironment environment, Object[] inputValues)
            throws StoredProcedureException, SQLException {

        boolean hasErrors = false;
        List<String> errorMessages = new LinkedList<>();
        errorMessages.add("\n");

        // Test if databaseName is valid
        String databaseName = (String) inputValues[0];
        if (StringUtils.isEmpty(databaseName)) {
            hasErrors = true;
            errorMessages.add("database_name can't be empty.");
        } else {
            try {
                ResultSet rs = environment.executeQuery(
                        "SELECT distinct database_name FROM CATALOG_VDP_METADATA_VIEWS() WHERE database_name = '" + databaseName + "'");
                // The query returns no rows if there is no database with the provided name
                if (!rs.next()) {
                    throw new StoredProcedureException();
                }
            } catch (StoredProcedureException e) {
                hasErrors = true;
                errorMessages.add("view_name = '" + databaseName + "' is not valid.");
            }
        }

        // Test if viewName is valid
        String viewName = (String) inputValues[1];
        boolean validView = true; // Used to know if it's necessary testing the lastUpdateCondition
        if (StringUtils.isEmpty(viewName)) {
            hasErrors = true;
            validView = false;
            errorMessages.add("view_name can't be empty.");
        } else {
            try {
                environment.executeQuery("select 1 from " + viewName + " fetch first 1 rows only CONTEXT ('cache' = 'on')");
            } catch (StoredProcedureException e) {
                hasErrors = true;
                validView = false;
                errorMessages.add("view_name = '" + viewName + "' is not valid.");
            }
        }

        // Test if lastUpdateCondition is valid
        String lastUpdateCondition = (String) inputValues[2];
        if (StringUtils.isEmpty(lastUpdateCondition)) {
            hasErrors = true;
            errorMessages.add("last_update_condition can't be empty.");
        } else if (validView) {
            try {
                environment.executeQuery("select 1 from " + viewName + " where " + lastUpdateCondition
                        + " fetch first 1 rows only CONTEXT ('cache' = 'on')");
            } catch (StoredProcedureException e) {
                hasErrors = true;
                errorMessages.add("last_update_condition = '" + lastUpdateCondition + "' is not valid.");
            }
        }

        // DEPRECATED as now pk is retrieved from GET_PRIMARY_KEYS stored procedure
        // Test if pkField is valid
        // String pkField = (String) inputValues[2];
        // if (StringUtils.isEmpty(pkField)) {
        // hasErrors = true;
        // errorMessages.add("pk_field can't be empty.");
        // } else {
        // try {
        // this.environment
        // .executeQuery("select " + pkField + " from " + viewName + " fetch first 1
        // rows only CONTEXT ('cache' = 'on')");
        // } catch (Exception e) {
        // hasErrors = true;
        // errorMessages.add("pk_field = '" + pkField + "' is not valid.");
        // }
        // }

        // Test if numElementsInClause is valid
        if (StringUtils.isNotBlank((String) inputValues[2])) {
            Integer numElementsInClause = null;
            try {
                numElementsInClause = Integer.valueOf((String) inputValues[3]);
                if (numElementsInClause.intValue() <= 0) {
                    hasErrors = true;
                    errorMessages.add("num_elements_in_clause must be greater than 0.");
                }
            } catch (Exception e) {
                hasErrors = true;
                errorMessages.add("num_elements_in_clause = " + inputValues[3] + " is not valid.");
            }
        } else {
            hasErrors = true;
            errorMessages.add("num_elements_in_clause can't be empty.");
        }

        if (hasErrors) {
            throw new StoredProcedureException(StringUtils.join(errorMessages, "\n"));
        }

    }

    public static List<String> getPkFieldsByViewNameAndDb(DatabaseEnvironment environment, String databaseName, String viewName)
            throws StoredProcedureException, SQLException {

        List<String> pkFields = new LinkedList<>();

        StringBuilder query = new StringBuilder();
        query.append(" SELECT distinct pk.column_name ");
        query.append(" FROM GET_PRIMARY_KEYS() pk ");
        query.append(" WHERE pk.database_name = '").append(databaseName).append("'");
        query.append(" AND pk.input_view_name = '").append(viewName).append("'");
        ResultSet rs = environment.executeQuery(query.toString());

        while (rs.next()) {
            pkFields.add(rs.getString(1));
        }

        rs.close();
        
        return pkFields;
    }

}
