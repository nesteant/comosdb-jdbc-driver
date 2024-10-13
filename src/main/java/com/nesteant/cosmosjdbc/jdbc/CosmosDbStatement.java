package com.nesteant.cosmosjdbc.jdbc;

import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.nesteant.cosmosjdbc.jdbc.models.CosmosSql;
import com.nesteant.cosmosjdbc.jdbc.resultset.AsyncCosmosSqlResultSet;
import com.nesteant.cosmosjdbc.jdbc.resultset.SimpleCosmosSqlResultSet;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.LinkedHashMap;

public class CosmosDbStatement implements Statement {

    private static final Logger log = LoggerFactory.getLogger(CosmosDbStatement.class);
    protected CosmosDbConnection connection;

    protected int maxRows = 20;
    protected int fetchSize = 20;
    protected ResultSet resultSet;

    public CosmosDbStatement(CosmosDbConnection connection) {
        this.connection = connection;
    }

    public CosmosDbStatement(CosmosDbConnection connection, int resultSetType, int resultSetConcurrency) {
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        log.info("Executing query: {}", sql);
        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();

        CosmosSql cosmosSql = transformSql(sql);
        CosmosPagedIterable<LinkedHashMap> objects = connection.currentDatabase.getContainer(cosmosSql.getContainer())
                .queryItems(cosmosSql.getSql(), options, LinkedHashMap.class);
        resultSet = AsyncCosmosSqlResultSet.create(connection, objects, this.fetchSize);
        log.info("Result set: {}", resultSet);
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        log.info("Executing update in db: {}", sql);
        return 0;
    }

    @Override
    public void close() throws SQLException {
        log.info("Closing statement");
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        log.info("Getting max field size");
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        log.info("Setting max field size: {}", max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        log.info("Getting max rows");
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        log.info("Setting max rows: {}", max);
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        log.info("Setting escape processing: {}", enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        log.info("Getting query timeout");
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        log.info("Setting query timeout: {}", seconds);
    }

    @Override
    public void cancel() throws SQLException {
        log.info("Cancelling statement");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        log.info("Getting warnings");
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        log.info("Clearing warnings");
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        log.info("Setting cursor name: {}", name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        log.info("Executing with no params: {}", sql);
        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();

        log.info(sql);
        CosmosSql cosmosSql = transformSql(sql);

        CosmosPagedIterable<LinkedHashMap> objects;
        if (cosmosSql.getContainer() != null) {
            objects = connection.currentDatabase.getContainer(cosmosSql.getContainer())
                    .queryItems(cosmosSql.getSql(), options, LinkedHashMap.class);
            resultSet = AsyncCosmosSqlResultSet.create(connection, objects, this.maxRows);
            return true;
        } else {
            CosmosPagedIterable<CosmosContainerProperties> cosmosContainerProperties = connection
                    .currentDatabase
                    .queryContainers(cosmosSql.getSql());
            //TODO: map query to result set
            cosmosContainerProperties.stream().forEach(t -> {
                String x = "";
            });
            String x = "";
        }
        resultSet = new SimpleCosmosSqlResultSet(connection);
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        log.info("Getting result set");
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        log.info("Getting update count");
        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        log.info("Getting more results");
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        log.info("Setting fetch direction: {}", direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        log.info("Getting fetch direction");
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        log.info("Setting fetch size: {}", rows);
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        log.info("Getting fetch size");
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        log.info("Getting result set concurrency");
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        log.info("Getting result set type");
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        log.info("Adding batch: {}", sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        log.info("Clearing batch");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        log.info("Executing batch");
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        log.info("Getting connection");
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        log.info("Getting more results: {}", current);
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        log.info("Getting generated keys");
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        log.info("Executing update in db with auto : {}, {}", sql, autoGeneratedKeys);
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        log.info("Executing update db with indexes: {}, {}", sql, columnIndexes);
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        log.info("Executing update in db with names: {}, {}", sql, columnNames);
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        log.info("Executing with keys: {} {}", sql, autoGeneratedKeys);
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        log.info("Executing with columnIndexes: {} {}", sql, columnIndexes);
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        log.info("Executing with column names: {} {}", sql, columnNames);
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        log.info("Getting result set holdability");
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        log.info("Checking if closed");
        return connection.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        log.info("Setting poolable: {}", poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        log.info("Checking if poolable");
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        log.info("Closing on completion");

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        log.info("Checking if close on completion");
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        log.info("Unwrapping statement");
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        log.info("Checking if statement is wrapper for: {}", iface);
        return false;
    }


    protected CosmosSql transformSql(String sql) throws SQLException {
        log.info("Transforming sql: {}", sql);

        net.sf.jsqlparser.statement.Statement parsed = null;
        try {
            parsed = CCJSqlParserUtil.parse(sql, ccjSqlParser -> {
                ccjSqlParser.withUnsupportedStatements(true);
            });
        } catch (JSQLParserException e) {
            throw new SQLException(e);
        }

        if (parsed instanceof PlainSelect) {
            Select select = (PlainSelect) parsed;

            PlainSelect plainSelect = select.getPlainSelect();

            boolean isTechnicalQuery = plainSelect.getFromItem() == null;

            if (isTechnicalQuery) {
                return new CosmosSql(sql, null, connection.currentDatabase.getId());
            }

            Table table = (Table) plainSelect.getFromItem();
            String tableName = table.getName().replaceAll("\"", "");
            String dbLinkName = table.getDBLinkName();
            String schemaName = table.getSchemaName();
            String alias = table.getAlias() != null ? table.getAlias().getName() : null;
            plainSelect.getSelectItems().stream().forEach(i -> {
                Expression expr = i.getExpression();

                if (expr instanceof AllTableColumns) {
                    SelectItem<AllColumns> casted = (SelectItem<AllColumns>) i;
                    AllTableColumns ch = (AllTableColumns) expr;
                    casted.setExpression(new AllColumns(ch.getExceptColumns(), ch.getReplaceExpressions()));
                }
            });
            table.setAlias(null);
            table.setSchemaName(null);
            Expression where = plainSelect.getWhere();
            String basicQuery = "select * FROM c";
            String whereString = "";
            if (where != null) {
                whereString += " WHERE " + where;
                log.info("Where: {}", whereString);
                basicQuery += " %s";
            }

            String formattedSql = String.format(basicQuery, whereString);
            log.info("Table: {} DBLinkName: {} SchemaName: {} Alias: {}. Cosmos sql {}", tableName, dbLinkName, schemaName, alias, formattedSql);
            return new CosmosSql(formattedSql, tableName, connection.currentDatabase.getId());
        }

        throw new SQLException("Unsupported SQL statement: " + sql);
    }
}
