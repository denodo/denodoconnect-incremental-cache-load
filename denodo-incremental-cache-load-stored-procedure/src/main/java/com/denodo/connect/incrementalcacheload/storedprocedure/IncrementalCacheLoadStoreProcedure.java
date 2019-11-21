package com.denodo.connect.incrementalcacheload.storedprocedure;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.denodo.connect.incrementalcacheload.storedprocedure.util.DBUtils;
import com.denodo.connect.incrementalcacheload.storedprocedure.util.InputParametersVO;
import com.denodo.connect.incrementalcacheload.storedprocedure.util.QueryListVO;
import com.denodo.connect.incrementalcacheload.storedprocedure.util.QueryParameters;
import com.denodo.connect.incrementalcacheload.storedprocedure.util.Utils;
import com.denodo.vdb.engine.storedprocedure.AbstractStoredProcedure;
import com.denodo.vdb.engine.storedprocedure.DatabaseEnvironment;
import com.denodo.vdb.engine.storedprocedure.DatabaseEnvironmentImpl;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureException;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureParameter;

/***
 * The Denodo Incremental Cache Load is a tool that allows the execution of an
 * incremental cache load on demand. It saves the current scenario of having to
 * perform two queries with one insert into cache per new/modified tuple to be
 * cached. This SP can perform this same operation in blocks, by asking VDP to
 * update the cache with SELECT queries using large blocks of "IN" conditions
 * (as large as allowed by the cached source) defined in the parameters of the
 * execution.
 *
 * @author acastro
 *
 */
public class IncrementalCacheLoadStoreProcedure extends AbstractStoredProcedure {

    private static final long serialVersionUID = 2998394943002628742L;
    private DatabaseEnvironment environment;

    public IncrementalCacheLoadStoreProcedure() {
    }

    /**
     * This method is invoked when stored procedure is initialized
     *
     * @param theEnvironment object that allows communicate with VDP server
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
     * Method where input and output parameters of the stored procedure are configured
     *
     * @return StoredProcedureParameter array with info about stored procedure parameters
     */
    @Override
    public StoredProcedureParameter[] getParameters() {
        return new StoredProcedureParameter[]{
            // Input parameters
            new StoredProcedureParameter("database_name", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN),
            new StoredProcedureParameter("view_name", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN),
            new StoredProcedureParameter("last_update_condition", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN),
            new StoredProcedureParameter("num_elements_in_clause", Types.VARCHAR,
                StoredProcedureParameter.DIRECTION_IN),
            // Output parameter
            new StoredProcedureParameter("num_updated_rows", Types.VARCHAR, StoredProcedureParameter.DIRECTION_OUT)};
    }

    /**
     * This method is invoked when the stored procedure is executed
     *
     * @param inputValues array with input parameters
     */
    @Override
    public void doCall(Object[] inputValues) throws StoredProcedureException {

        long start = System.nanoTime();
        log(LOG_DEBUG, "START of the Incremental Cache Load SP.");

        try {

            final DatabaseEnvironmentImpl databaseEnvironmentImpl = (DatabaseEnvironmentImpl) getEnvironment();

            // Input parameter and cache validation
            long startAux = System.nanoTime();
            Utils.validateInputParametersAndCache(environment, databaseEnvironmentImpl, inputValues);
            long endAux = System.nanoTime();
            double seconds = (endAux - startAux) / 1000000000.0;
            log(LOG_TRACE, "Time elapsed during validation: \t " + seconds + " seconds.");

            // Initialization of variables
            InputParametersVO inputParameters = new InputParametersVO((String) inputValues[0], (String) inputValues[1],
                (String) inputValues[2], Integer.valueOf((String) inputValues[3]));

            log(LOG_DEBUG, "Input parameters: " + inputParameters.toString());

            // Get view PK
            List<String> pkFields = Utils.getPkFieldsByViewNameAndDb(environment,
                inputParameters.getDatabaseName().replaceAll("\"", ""),
                inputParameters.getViewName().replaceAll("\"", ""));

            // Calculate the PK fields that were updated since the lastUpdateCondition
            // Distinct clause added as it is not guaranteed that the view PK has no repeated
            // values

            // Creation of the array of queries to be executed to update the cache
            log(LOG_TRACE, "BEGIN of the building of the query array.");
            startAux = System.nanoTime();

            QueryListVO queryListVO = getQueryList(inputParameters, pkFields);

            endAux = System.nanoTime();
            seconds = (endAux - startAux) / 1000000000.0;
            log(LOG_TRACE, "END of the building of the query array: \t" + seconds + " seconds.");

            // Cache update
            log(LOG_TRACE, "START of cache update");
            startAux = System.nanoTime();

            executeUpdateCache(queryListVO.getQueryList());

            endAux = System.nanoTime();
            seconds = (endAux - startAux) / 1000000000.0;
            log(LOG_TRACE, "END of cache update: \t" + seconds + " seconds.");

            // Add a row with the stored procedure out parameter as the stored procedure
            // result
            getProcedureResultSet()
                .addRow(new Object[]{"Cache Refreshed Successfully. Updated rows (distinct PK values):"
                    + queryListVO.getRowCount()});

        } catch (StoredProcedureException e) {
            this.environment.log(LOG_ERROR, e.getMessage());
            throw e;
        } catch (Exception e) {
            this.environment.log(LOG_ERROR, e.getMessage());
            throw new StoredProcedureException(e);
        } finally {
            long end = System.nanoTime();
            double seconds = (end - start) / 1000000000.0;
            log(LOG_DEBUG, "END of the Incremental Cache Load SP. Time elapsed: \t " + seconds + " seconds.");
        }

    }

    private void executeUpdateCache(List<QueryParameters> queryList) throws StoredProcedureException {

        ResultSet aux = null;
        int i = 1;

        for (QueryParameters q : queryList) {
            try {
                long iniCache = System.nanoTime();
                aux = this.environment.executeQuery(q.getQuery(), q.getParameters());
                long finCache = System.nanoTime();
                double seconds = (finCache - iniCache) / 1000000000.0;
                log(LOG_TRACE, "Query " + i + "\t: " + seconds + " seconds.");
                i++;
                aux.next();
            } catch (SQLException | StoredProcedureException e) {
                log(LOG_DEBUG, "ERROR in executeUpdateCache(): Query - " + q + ". " + e);
                throw new StoredProcedureException("ERROR executing query update of cache:", e);
            } finally {
                DBUtils.closeRs(aux);
            }
        }
    }


    private QueryListVO getQueryList(InputParametersVO inputParameters, List<String> pkFields)
        throws StoredProcedureException {

        long startAux = System.nanoTime();
        // This query obtains all the PK values that match the input condition.
        // They will be used to create the queries to update the cache.
        String query =
            "SELECT DISTINCT " + Utils.join(pkFields, ", ") + " FROM " + inputParameters.getDatabaseName() + "."
                + inputParameters.getViewName() + " WHERE " + inputParameters.getLastUpdateCondition()
                + " CONTEXT('cache'='off')";

        int rowCount = 0;
        List<QueryParameters> queryList = new ArrayList<>();
        ResultSet rs = null;
        try {

            log(LOG_DEBUG, "getQueryList(): query = " + query);
            rs = this.environment.executeQuery(query);
            long endAux = System.nanoTime();
            double seconds = (endAux - startAux) / 1000000000.0;
            log(LOG_TRACE, "Time elapsed recovering PKs: \t " + seconds + " seconds.");


            //  Variables used to create the query list
            // Stores the value of the current parameter
            Object rowValue;
            // Used to add a number of elements equal to the 'num_elements_in_clause' input SP parameter
            int pkChunkRowCount = 0;
            // List of parameters that will be passed in the query execution
            List<Object> parameters = new ArrayList<>();
            // List of question marks for the IN clause
            StringBuilder inClauseParameters = new StringBuilder();
            boolean firstParameter = true;
            // Contains the list of OR clauses for the multiple PK cases
            StringBuilder orClauseParameters = new StringBuilder();
            String orClause = null;
            boolean singlePk = pkFields.size() == 1;

            while (rs.next()) {

                rowCount++;
                pkChunkRowCount++;

                // We have two ways of building the cache update queries:
                //  1) The PK is simple -> IN clause with a list of parameters (?)
                //  2) The PK is multiple -> Sequence of OR clauses with (pk_field1 = val1 AND pk_field2 = val2...)
                if (singlePk) {

                    // PK is one only field
                    rowValue = rs.getObject(1) != null ? rs.getObject(1) : "";
                    parameters.add(rowValue);
                    if (firstParameter) {
                        firstParameter = false;
                    } else {
                        inClauseParameters.append(",");
                    }
                    inClauseParameters.append("?");

                    if (pkChunkRowCount == inputParameters.getNumElementsInClause() || rs.isLast()) {
                        query =
                            "SELECT * FROM " + inputParameters.getDatabaseName() + "." + inputParameters.getViewName()
                                + " WHERE "
                                + pkFields.get(0) + " IN (" + inClauseParameters.toString() + ") "
                                + "CONTEXT('cache_preload'='true','cache_invalidate'='matching_rows',"
                                + "'returnqueryresults'='false','cache_wait_for_load'='true')";

                        queryList.add(buildQueryParameters(query, parameters));

                        // Reset aux variables
                        firstParameter = true;
                        pkChunkRowCount = 0;
                        parameters.clear();
                        inClauseParameters.setLength(0);
                    }

                } else {

                    // PK has two or more fields
                    if (orClause == null) {
                        orClause = createOrClause(pkFields);
                    }
                    buildOrCondition(pkFields, rs, orClauseParameters, orClause, parameters);

                    if (pkChunkRowCount == inputParameters.getNumElementsInClause() || rs.isLast()) {

                        // Cache refresh of PK Chunk
                        query = "SELECT * FROM " + inputParameters.getDatabaseName() + "." + inputParameters.getViewName()
                            + " WHERE " + orClauseParameters.toString()
                            + " CONTEXT('cache_preload'='true','cache_invalidate'='matching_rows',"
                            + " 'returnqueryresults'='false','cache_wait_for_load'='true')";

                        queryList.add(buildQueryParameters(query, parameters));

                        // Reset aux variables
                        pkChunkRowCount = 0;
                        orClauseParameters = new StringBuilder();
                        parameters.clear();

                    } else {
                        // There are more conditions to append
                        orClauseParameters.append("OR");
                    }
                }
            }
        } catch (Exception e) {
            log(LOG_DEBUG, "ERROR in getQueryList(): " + e);
            throw new StoredProcedureException("ERROR getting rows to update in cache.", e);
        } finally {
            // Close resources
            DBUtils.closeRs(rs);
        }

        log(LOG_DEBUG, "getQueryList(): queryList = " + queryList.toString());
        return new QueryListVO(rowCount, queryList);
    }

    private QueryParameters buildQueryParameters(String query, List<Object> parameters) {
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setQuery(query);
        queryParameters.setParameters(parameters.toArray());
        return queryParameters;
    }

    private String createOrClause(List<String> pkFields) {
        List<String> conditions = new ArrayList<>();
        for (String pkField : pkFields) {
            conditions.add(pkField + " = ? ");
        }
        // Build an OR clause with all the PK elements splitted by the AND clause
        return " (" + Utils.join(conditions, " AND ") + ") ";
    }

    private void buildOrCondition(List<String> pkFields, ResultSet rs,
        StringBuilder orClauseParameters, String orClause, List<Object> parameters) throws SQLException {

        // 1. Add OR clause to where
        // The OR clause is always the same. We only need to calculate it once and then append it the requested times
        orClauseParameters.append(orClause);

        // 2. Add parameter values to parameter list
        for (int i = 0; i < pkFields.size(); i++) {
            parameters.add(rs.getObject(i + 1));
        }
    }

    @Override
    public int getNumOfAffectedRows() {
        // Deprecated
        return 0;
    }

}
