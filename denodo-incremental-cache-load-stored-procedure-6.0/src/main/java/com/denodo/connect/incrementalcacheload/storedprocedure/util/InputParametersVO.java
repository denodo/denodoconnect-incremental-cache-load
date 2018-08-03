package com.denodo.connect.incrementalcacheload.storedprocedure.util;

public class InputParametersVO {

    private String databaseName;
    private String viewName;
    private String lastUpdateCondition;
    private Integer numElementsInClause;

    public InputParametersVO() {
        super();
    }

    public InputParametersVO(String databaseName, String viewName, String lastUpdateCondition, Integer numElementsInClause) {
        super();
        this.databaseName = databaseName;
        this.viewName = viewName;
        this.lastUpdateCondition = lastUpdateCondition;
        this.numElementsInClause = numElementsInClause;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getViewName() {
        return this.viewName;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public String getLastUpdateCondition() {
        return this.lastUpdateCondition;
    }

    public void setLastUpdateCondition(String lastUpdateCondition) {
        this.lastUpdateCondition = lastUpdateCondition;
    }

    public Integer getNumElementsInClause() {
        return this.numElementsInClause;
    }

    public void setNumElementsInClause(Integer numElementsInClause) {
        this.numElementsInClause = numElementsInClause;
    }

}
