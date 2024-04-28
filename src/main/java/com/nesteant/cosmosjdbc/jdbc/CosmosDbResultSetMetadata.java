package com.nesteant.cosmosjdbc.jdbc;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

@Slf4j
public class CosmosDbResultSetMetadata implements ResultSetMetaData {

    private final CosmosDbResultSet resultSet;

    public CosmosDbResultSetMetadata(CosmosDbResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return resultSet.columns.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        log.info("Getting isAutoIncrement from result set: {}", column);
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        log.info("Getting isCaseSensitive from result set: {}", column);
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        log.info("Getting isSearchable from result set: {}", column);
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        log.info("Getting isCurrency from result set: {}", column);
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        log.info("Getting isNullable from result set: {}", column);
        return ResultSetMetaData.columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        log.info("Getting isSigned from result set: {}", column);
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        log.info("Getting getColumnDisplaySize from result set: {}", column);
        return 100;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        String columnLabel = resultSet.columns.get(column - 1);
        log.info("Getting getColumnLabel from result set: {}, {}", column, columnLabel);
        return columnLabel;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        String columnName = resultSet.columns.get(column - 1);
        log.info("Getting getColumnName from result set: {}, {}", column, columnName);
        return columnName;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        log.info("Getting getSchemaName from result set: {}", column);
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        log.info("Getting getPrecision from result set: {}", column);
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        log.info("Getting getScale from result set: {}", column);
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        log.info("Getting getTableName from result set: {}", column);
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        log.info("Getting getCatalogName from result set: {}", column);
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        log.info("Getting getColumnType from result set: {}", column);
        return Types.VARCHAR;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        log.info("Getting getColumnTypeName from result set: {}", column);
        return "";
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        log.info("Getting isReadOnly from result set: {}", column);
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        log.info("Getting isWritable from result set: {}", column);
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        log.info("Getting isDefinitelyWritable from result set: {}", column);
        return true;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        log.info("Getting getColumnClassName from result set: {}", column);
        return "";
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        log.info("Unwrapping result set metadata");
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        log.info("Checking if result set metadata is wrapper for: {}", iface);
        return false;
    }
}
