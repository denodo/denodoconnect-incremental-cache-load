package com.denodo.connect.incrementalcacheload.storedprocedure;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.denodo.vdb.engine.storedprocedure.AbstractStoredProcedure;
import com.denodo.vdb.engine.storedprocedure.DatabaseEnvironment;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureException;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureParameter;

public class IncrementalCacheLoadStoreProcedure extends AbstractStoredProcedure {

    private static final long serialVersionUID = 2998394943002628742L;
    private DatabaseEnvironment environment;

    public IncrementalCacheLoadStoreProcedure() {
    }

    /**
     * This method is invoked when stored procedure is initialized
     *
     * @param theEnvironment
     *            object that allows communicate with VDP server
     */
    @Override
    public void initialize(DatabaseEnvironment theEnvironment) {
        super.initialize(theEnvironment);
        this.environment = theEnvironment;
    }

    /**
     * Gets store procedure description
     *
     * @return String
     */
    @Override
    public String getDescription() {
        return "Performs an incremental cache load of a given view";
    }

    /**
     * Gets store procedure name
     *
     * @return String
     */
    @Override
    public String getName() {
        return IncrementalCacheLoadStoreProcedure.class.getName();
    }

    /**
     * Method where input and output parameters of the stored procedure are
     * configured
     *
     * @return StoredProcedureParameter array with info about stored procedure
     *         parameters
     */
    @Override
    public StoredProcedureParameter[] getParameters() {
        return new StoredProcedureParameter[] {
                // Input parameters
                new StoredProcedureParameter("view_name", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN),
                new StoredProcedureParameter("last_update_condition", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN),
                new StoredProcedureParameter("pk_field", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN),
                new StoredProcedureParameter("num_elements_in_clause", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN),
                // Output parameter
                new StoredProcedureParameter("num_updated_rows", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT) };
    }

    /**
     * This method is invoked when the stored procedure is executed
     *
     * @param inputValues
     *            array with input parameters
     */
    @Override
    public void doCall(Object[] inputValues) throws StoredProcedureException {

        long start = System.nanoTime();
        log(LOG_DEBUG, "START of the Incremental Cache Load SP.");

        try {

            // Parameter validation
            long startAux = System.nanoTime();
            validateParameters(inputValues);
            long endAux = System.nanoTime();
            double seconds = (endAux - startAux) / 1000000000.0;
            log(LOG_TRACE, "Time elapsed during parameter validation: \t " + seconds + " seconds.");

            // Initialization of variables
            String viewName = (String) inputValues[0];
            String lastUpdateCondition = (String) inputValues[1];
            String pkField = (String) inputValues[2];
            Integer numElementsInClause = Integer.valueOf((String) inputValues[3]);
            
            String rowValue = StringUtils.EMPTY;
            String inClauseString = StringUtils.EMPTY;
            int pkChunkRowCount = 0;
            int rowCount = 0;
            List<String> pkValuesChunk = new LinkedList<String>();
            List<String> queryList = new ArrayList<>();

            // Calculate the PK fields that were updates since the lastUpdateCondition
            startAux = System.nanoTime();
            String query = "SELECT " + pkField + " FROM " + viewName + " WHERE " + lastUpdateCondition + " CONTEXT('cache'='off')";
            ResultSet rs = this.environment.executeQuery(query);
            endAux = System.nanoTime();
            seconds = (endAux - startAux) / 1000000000.0;
            log(LOG_TRACE, "Time elapsed recovering PKs: \t " + seconds + " seconds.");

            // Creation of the array of queries to be executed to update the cache
            log(LOG_TRACE, "BEGIN of the building of the query array.");
            startAux = System.nanoTime();
            while (rs.next()) {

                rowCount++;
                pkChunkRowCount++;
                rowValue = rs.getObject(1).toString();
                pkValuesChunk.add(rowValue);

                if (pkChunkRowCount == numElementsInClause.intValue() || rs.isLast()) {

                    // Creation of the IN clause with the specified chunk: numElementsInClause
                    inClauseString = StringUtils.join(pkValuesChunk, ",");

                    // Cache refresh of PK Chunk
                    query = "SELECT * FROM " + viewName + " WHERE " + pkField + " IN (" + inClauseString + ") "
                            + "CONTEXT('cache_preload'='true','cache_invalidate'='matching_rows',"
                            + "'returnqueryresults'='false','cache_wait_for_load'='true')";

                    queryList.add(query);

                    // Reset the chunk
                    pkChunkRowCount = 0;
                    pkValuesChunk.clear();
                }
            }

            rs.close();
            endAux = System.nanoTime();
            seconds = (endAux - startAux) / 1000000000.0;
            log(LOG_TRACE, "END of the building of the query array: \t" + seconds + " seconds.");

            // Cache update
            // NOTE: This way (outside the "main" resultSet) solves blocks with size 1
            // resources problem
            int i = 1;
            log(LOG_TRACE, "START of cache update");
            for (String q : queryList) {
                long iniCache = System.nanoTime();
                final ResultSet aux = this.environment.executeQuery(q);
                long finCache = System.nanoTime();
                seconds = (finCache - iniCache) / 1000000000.0;
                log(LOG_TRACE, "Query " + i + "\t: " + seconds + " seconds.");
                i++;
                aux.next();
                aux.close();
            }
            log(LOG_TRACE, "END of cache update");

            // Add a row with the stored procedure out parameter as the stored procedure
            // result
            getProcedureResultSet().addRow(new Object[] { new String("Cache Refreshed Successfully. Updated rows:" + rowCount) });

        } catch (StoredProcedureException | SecurityException | IllegalStateException | SQLException e) {
            this.environment.log(LOG_ERROR, e.getMessage());
            throw new StoredProcedureException(e);
        }

        long end = System.nanoTime();
        double seconds = (end - start) / 1000000000.0;
        log(LOG_DEBUG, "END of the Incremental Cache Load SP. Time elapsed: \t " + seconds + " seconds.");

    }

    // TODO: Improve strategy and avoid cyclomatic complexity
    private void validateParameters(Object[] inputValues)
            throws StoredProcedureException {
        
        boolean hasErrors = false;
        List<String> errorMessages = new LinkedList<>();
        errorMessages.add("\n");

        // Test if viewName is valid
        String viewName = (String) inputValues[0];
        if (StringUtils.isEmpty(viewName)) {
            hasErrors = true;
            errorMessages.add("view_name can't be empty.");
        } else {
            try {
                this.environment.executeQuery("select 1 from " + viewName + " fetch first 1 rows only CONTEXT ('cache' = 'on')");
            } catch (StoredProcedureException e) {
                hasErrors = true;
                errorMessages.add("view_name = '" + viewName + "' is not valid.");
            }
        }

        // Test if lastUpdateCondition is valid
        String lastUpdateCondition = (String) inputValues[1];
        if (StringUtils.isEmpty(lastUpdateCondition)) {
            hasErrors = true;
            errorMessages.add("last_update_condition can't be empty.");
        } else {
            try {
                this.environment.executeQuery(
                        "select 1 from " + viewName + " where " + lastUpdateCondition + " fetch first 1 rows only CONTEXT ('cache' = 'on')");
            } catch (StoredProcedureException e) {
                hasErrors = true;
                errorMessages.add("last_update_condition = '" + lastUpdateCondition + "' is not valid.");
            }
        }

        // Test if pkField is valid
        String pkField = (String) inputValues[2];
        if (StringUtils.isEmpty(pkField)) {
            hasErrors = true;
            errorMessages.add("pk_field can't be empty.");
        } else {
            try {
                this.environment
                        .executeQuery("select " + pkField + " from " + viewName + " fetch first 1 rows only CONTEXT ('cache' = 'on')");
            } catch (Exception e) {
                hasErrors = true;
                errorMessages.add("pk_field = '" + pkField + "' is not valid.");
            }
        }

        // Test if numElementsInClause is valid
        if (inputValues[3] != null) {
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

    @Override
    public int getNumOfAffectedRows() {
        // Deprecated
        return 0;
    }

}
