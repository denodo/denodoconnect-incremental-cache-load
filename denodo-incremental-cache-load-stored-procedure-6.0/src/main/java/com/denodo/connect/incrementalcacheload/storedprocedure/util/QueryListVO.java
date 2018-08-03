package com.denodo.connect.incrementalcacheload.storedprocedure.util;

import java.util.List;

public class QueryListVO {

    private int rowCount;
    private List<String> queryList;

    public QueryListVO() {
        super();
    }

    public QueryListVO(int rowCount, List<String> queryList) {
        super();
        this.rowCount = rowCount;
        this.queryList = queryList;
    }

    public int getRowCount() {
        return this.rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public List<String> getQueryList() {
        return this.queryList;
    }

    public void setQueryList(List<String> queryList) {
        this.queryList = queryList;
    }

}
