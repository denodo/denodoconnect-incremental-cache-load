package com.denodo.connect.incrementalcacheload.storedprocedure.util;

import java.util.List;

public class QueryListVO {
    private int rowCount;
    private List<QueryParameters> queryList;

    public QueryListVO() {
        super();
    }

    public QueryListVO(int rowCount,
        List<QueryParameters> queryList) {
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