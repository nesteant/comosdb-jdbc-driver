package com.nesteant.cosmosjdbc.jdbc.resultset;

public interface CosmosSqlResultSet extends java.sql.ResultSet {
    int getColumnCount();

    String getColumnName(int column);

    int getColumnType(int column);
}
