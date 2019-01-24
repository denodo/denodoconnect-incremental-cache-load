package com.denodo.connect.incrementalcacheload.storedprocedure;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.denodo.connect.incrementalcacheload.storedprocedure.util.DBUtils;
import com.denodo.connect.incrementalcacheload.storedprocedure.util.InputParametersVO;
import com.denodo.connect.incrementalcacheload.storedprocedure.util.QueryListVO;
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
                new StoredProcedureParameter("database_name", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN),
                new StoredProcedureParameter("view_name", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN),
                new StoredProcedureParameter("last_update_condition", Types.VARCHAR, StoredProcedureParameter.DIRECTION_IN),
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
                    .addRow(new Object[] { new String("Cache Refreshed Successfully. Updated rows:"
                            + queryListVO.getRowCount()) });

        } catch (StoredProcedureException | SecurityException | IllegalStateException | SQLException e) {
            this.environment.log(LOG_ERROR, e.getMessage());
            throw new StoredProcedureException(e);
        }

        long end = System.nanoTime();
        double seconds = (end - start) / 1000000000.0;
        log(LOG_DEBUG, "END of the Incremental Cache Load SP. Time elapsed: \t " + seconds + " seconds.");

    }

    private void executeUpdateCache(List<String> queryList) throws StoredProcedureException {

        ResultSet aux = null;
        int i = 1;

        for (String q : queryList) {
            try {
                long iniCache = System.nanoTime();
                aux = this.environment.executeQuery(q);
                long finCache = System.nanoTime();
                double seconds = (finCache - iniCache) / 1000000000.0;
                log(LOG_TRACE, "Query " + i + "\t: " + seconds + " seconds.");
                i++;
                aux.next();                    
            } catch (SQLException | StoredProcedureException e) {
                log(LOG_DEBUG, "ERROR in executeUpdateCache(): Query - " + q + ". " + e.getMessage());
                throw new StoredProcedureException("ERROR executing query update of cache:" + e.getMessage());
            } finally {
                DBUtils.closeRs(aux);                    
            }
        }
    }

    @SuppressWarnings("boxing")
    private QueryListVO getQueryList(InputParametersVO inputParameters, List<String> pkFields) throws StoredProcedureException {

        String rowValue = "";
        String inClauseString = "";
        int pkChunkRowCount = 0;
        int rowCount = 0;
        List<String> pkValuesChunk = new LinkedList<String>();
        List<String> queryList = new ArrayList<>();

        long startAux = System.nanoTime();

        String query = "SELECT DISTINCT " + Utils.join(pkFields, ", ") + " FROM " + inputParameters.getDatabaseName() + "."
                + inputParameters.getViewName() + " WHERE " + inputParameters.getLastUpdateCondition() + " CONTEXT('cache'='off')";

        ResultSet rs = null;
        try {

            rs = this.environment.executeQuery(query);
            long endAux = System.nanoTime();
            double seconds = (endAux - startAux) / 1000000000.0;
            log(LOG_TRACE, "Time elapsed recovering PKs: \t " + seconds + " seconds.");

            // Variable used to check if we need quotes around the values in the IN clause
            // in singlePK context
            boolean singlePk = pkFields.size() == 1 ? true : false;
            Boolean isNumericPK = null;

            while (rs.next()) {

                rowCount++;
                pkChunkRowCount++;

                if (singlePk) {

                    // PK is one only field

                    // We check the type only once
                    if (isNumericPK == null) {
                        isNumericPK = Boolean.valueOf(rs.getObject(1) instanceof Number);
                    }

                    rowValue = rs.getObject(1).toString();
                    if (!isNumericPK) {
                        // If the type is not numeric, we surround the value with quotes
                        rowValue = "'" + rowValue + "'";
                    }
                    pkValuesChunk.add(rowValue);

                    if (pkChunkRowCount == inputParameters.getNumElementsInClause().intValue() || rs.isLast()) {

                        // Creation of the IN clause with the specified chunk: numElementsInClause
                        inClauseString = Utils.join(pkValuesChunk, ",");

                        // Cache refresh of PK Chunk
                        query = "SELECT * FROM " + inputParameters.getDatabaseName() + "." + inputParameters.getViewName() + " WHERE "
                                + pkFields.get(0) + " IN (" + inClauseString + ") "
                                + "CONTEXT('cache_preload'='true','cache_invalidate'='matching_rows',"
                                + "'returnqueryresults'='false','cache_wait_for_load'='true')";

                        queryList.add(query);

                        // Reset the chunk
                        pkChunkRowCount = 0;
                        pkValuesChunk.clear();
                    }

                } else {

                    // PK has two or more fields. We build a "full key" concatenating them all,
                    // using "-" as separator.
                    StringBuilder pkJoined = new StringBuilder();
                    pkJoined.append("'");
                    for (int i = 1; i <= pkFields.size(); i++) {
                        pkJoined.append(rs.getObject(i) != null? rs.getObject(i).toString() : "");
                        if (i < pkFields.size()) {
                            pkJoined.append("-");
                        }
                    }
                    pkJoined.append("'");
                    pkValuesChunk.add(pkJoined.toString());

                    if (pkChunkRowCount == inputParameters.getNumElementsInClause().intValue() || rs.isLast()) {

                        // Creation of the IN clause with the specified chunk: numElementsInClause
                        inClauseString = Utils.join(pkValuesChunk, ",");

                        // Cache refresh of PK Chunk
                        query = "SELECT * FROM " + inputParameters.getDatabaseName() + "." + inputParameters.getViewName()
                                + " WHERE concat(" + Utils.join(pkFields, ", '-', ") + ") IN (" + inClauseString + ") "
                                + "CONTEXT('cache_preload'='true','cache_invalidate'='matching_rows',"
                                + "'returnqueryresults'='false','cache_wait_for_load'='true')";

                        queryList.add(query);

                        // Reset the chunk
                        pkChunkRowCount = 0;
                        pkValuesChunk.clear();
                    }
                }
            }
        } catch (Exception e) {
            log(LOG_DEBUG, "ERROR in getQueryList(): " + e.getStackTrace());
            throw new StoredProcedureException("ERROR getting rows to update in cache." + e.getMessage());
        } finally {
            // Close resources
            DBUtils.closeRs(rs);
        }

        return new QueryListVO(rowCount, queryList);
    }

    @Override
    public int getNumOfAffectedRows() {
        // Deprecated
        return 0;
    }

}
