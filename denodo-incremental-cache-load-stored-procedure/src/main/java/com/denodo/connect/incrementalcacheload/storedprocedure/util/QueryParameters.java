package com.denodo.connect.incrementalcacheload.storedprocedure.util;

public class QueryParameters {

    private String query;
    private Object[] parameters;

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Object[] getParameters() {
        return parameters;
    }

    public void setParameters(Object[] parameters) {
        this.parameters = parameters;
    }
}
