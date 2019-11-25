package com.denodo.connect.incrementalcacheload.storedprocedure.util;

import com.denodo.vdb.engine.storedprocedure.StoredProcedureException;

public class IncrementalCacheLoadStoreProcedureException extends StoredProcedureException {

    private int updatedRows;

    public IncrementalCacheLoadStoreProcedureException() {
        super();
    }

    public IncrementalCacheLoadStoreProcedureException(String message, Throwable e, int updatedRows) {
        super(message, e);
        this.updatedRows = updatedRows;
    }

    public int getUpdatedRows() {
        return updatedRows;
    }
}
