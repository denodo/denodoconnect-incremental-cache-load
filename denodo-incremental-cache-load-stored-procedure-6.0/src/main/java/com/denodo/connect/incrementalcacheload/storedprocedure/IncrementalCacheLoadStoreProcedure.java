package com.denodo.connect.incrementalcacheload.storedprocedure;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

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

            // Input parameter validation
            long startAux = System.nanoTime();
            Utils.validateInputParameters(environment, databaseEnvironmentImpl, inputValues);
            long endAux = System.nanoTime();
            double seconds = (endAux - startAux) / 1000000000.0;
            log(LOG_TRACE, "Time elapsed during parameter validation: \t " + seconds + " seconds.");

            // Initialization of variables
            String databaseName = (String) inputValues[0];
            String viewName = (String) inputValues[1];
            String lastUpdateCondition = (String) inputValues[2];
            Integer numElementsInClause = Integer.valueOf((String) inputValues[3]);
            
            // Check if the cache is enabled before updating
            boolean isCacheEnabled = databaseEnvironmentImpl.isCacheEnabled(databaseName);
            
            if (!isCacheEnabled) {
                throw new StoredProcedureException("The cache is not enabled in the Server.");                
            } 
            
            String rowValue = StringUtils.EMPTY;
            String inClauseString = StringUtils.EMPTY;
            int pkChunkRowCount = 0;
            int rowCount = 0;
            List<String> pkValuesChunk = new LinkedList<String>();
            List<String> queryList = new ArrayList<>();

            // Get view PK
            startAux = System.nanoTime();
            List<String> pkFields = Utils.getPkFieldsByViewNameAndDb(environment, databaseName, viewName);

            if (pkFields == null || pkFields.isEmpty()) {
                throw new StoredProcedureException(
                        "The view " + viewName + " from the DB " + databaseName + " has no primary key. Cache won't be updated");
            }

            boolean singlePk = pkFields.size() == 1 ? true : false;

            // Calculate the PK fields that were updated since the lastUpdateCondition
            // Distinct clause added as is not guaranteed that the view PK has no repeated values
            String query = "SELECT DISTINCT " + StringUtils.join(pkFields, ", ") + " FROM "  + databaseName + "." + viewName +  " WHERE " + lastUpdateCondition
                    + " CONTEXT('cache'='off')";
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

                if (singlePk) {

                    // PK is one only field

                    rowValue = rs.getObject(1).toString();
                    pkValuesChunk.add("'" + rowValue + "'");

                    if (pkChunkRowCount == numElementsInClause.intValue() || rs.isLast()) {

                        // Creation of the IN clause with the specified chunk: numElementsInClause
                        inClauseString = StringUtils.join(pkValuesChunk, ",");

                        // Cache refresh of PK Chunk
                        query = "SELECT * FROM " + databaseName + "." + viewName + " WHERE " + pkFields.get(0) + " IN (" + inClauseString + ") "
                                + "CONTEXT('cache_preload'='true','cache_invalidate'='matching_rows',"
                                + "'returnqueryresults'='false','cache_wait_for_load'='true')";

                        queryList.add(query);

                        // Reset the chunk
                        pkChunkRowCount = 0;
                        pkValuesChunk.clear();
                    }

                } else {

                    // PK has two or more fields. We build a "full key" concatenating them all, using "-" as separator.

                    StringBuilder pkJoined = new StringBuilder();
                    pkJoined.append("'");
                    for (int i = 1; i <= pkFields.size(); i++) {
                        pkJoined.append(rs.getObject(i).toString());
                        if (i < pkFields.size()) {
                            pkJoined.append("-");
                        }
                    }
                    pkJoined.append("'");
                    pkValuesChunk.add(pkJoined.toString());

                    if (pkChunkRowCount == numElementsInClause.intValue() || rs.isLast()) {

                        // Creation of the IN clause with the specified chunk: numElementsInClause
                        inClauseString = StringUtils.join(pkValuesChunk, ",");

                        // Cache refresh of PK Chunk
                        query = "SELECT * FROM " + databaseName + "." + viewName + " WHERE concat(" + StringUtils.join(pkFields, ", '-', ") + ") IN ("
                                + inClauseString + ") " + "CONTEXT('cache_preload'='true','cache_invalidate'='matching_rows',"
                                + "'returnqueryresults'='false','cache_wait_for_load'='true')";

                        queryList.add(query);

                        // Reset the chunk
                        pkChunkRowCount = 0;
                        pkValuesChunk.clear();
                    }
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

    @Override
    public int getNumOfAffectedRows() {
        // Deprecated
        return 0;
    }

}
