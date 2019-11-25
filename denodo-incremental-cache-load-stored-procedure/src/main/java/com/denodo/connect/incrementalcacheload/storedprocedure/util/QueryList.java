package com.denodo.connect.incrementalcacheload.storedprocedure.util;

import java.util.List;

public class QueryList {
    private int rowCount;
    private List<QueryParameters> queryList;

    public QueryList() {
        super();
    }

    public QueryList(int rowCount, List<QueryParameters> queryList) {
        this.rowCount = rowCount;
        this.queryList = queryList;
    }

    public int getRowCount() {
        return this.rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public List<QueryParameters> getQueryList() {
        return queryList;
    }

    public void setQueryList(
        List<QueryParameters> queryList) {
        this.queryList = queryList;
    }
}