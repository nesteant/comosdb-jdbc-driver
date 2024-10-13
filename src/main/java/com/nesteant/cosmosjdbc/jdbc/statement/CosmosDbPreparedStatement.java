package com.nesteant.cosmosjdbc.jdbc.statement;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.nesteant.cosmosjdbc.jdbc.CosmosDbConnection;
import com.nesteant.cosmosjdbc.jdbc.CosmosDbStatement;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.util.*;

@Slf4j
public class CosmosDbPreparedStatement extends CosmosDbStatement implements PreparedStatement {

    private String sql;

    private Map<Integer, Object> changes = new LinkedHashMap<>();

    public CosmosDbPreparedStatement(CosmosDbConnection connection, String sql) {
        super(connection);
        this.sql = sql;
    }

    public CosmosDbPreparedStatement(CosmosDbConnection connection, String sql, int resultSetType, int resultSetConcurrency) {
        super(connection, resultSetType, resultSetConcurrency);
        this.sql = sql;
    }

    public CosmosDbPreparedStatement(CosmosDbConnection connection, String sql, int[] columnIndexes) {
        super(connection);
        this.sql = sql;
    }

    public CosmosDbPreparedStatement(CosmosDbConnection connection, String sql, String[] columnNames) {
        super(connection);
        this.sql = sql;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        log.info("Executing query: {}", sql);
        return this.executeQuery(sql);
    }

    @Override
    public int executeUpdate() throws SQLException {
        log.info("Executing update prep: {}", sql);
        executeCosmosSqlUpdate(sql);
        return 1;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        log.info("Setting null at index: {}", parameterIndex);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        log.info("Setting boolean at index: {}", parameterIndex);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        log.info("Setting byte at index: {}", parameterIndex);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        log.info("Setting short at index: {}", parameterIndex);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        log.info("Setting int at index: {}", parameterIndex);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        log.info("Setting long at index: {}", parameterIndex);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        log.info("Setting float at index: {}", parameterIndex);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        log.info("Setting double at index: {}", parameterIndex);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        log.info("Setting big decimal at index: {}", parameterIndex);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        log.info("Setting string at index: {}", parameterIndex);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        log.info("Setting bytes at index: {}", parameterIndex);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        log.info("Setting date at index: {}", parameterIndex);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        log.info("Setting time at index: {}", parameterIndex);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        log.info("Setting timestamp at index: {}", parameterIndex);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        log.info("Setting ascii stream at index: {}", parameterIndex);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        log.info("Setting unicode stream at index: {}", parameterIndex);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        log.info("Setting binary stream at index: {}", parameterIndex);
    }

    @Override
    public void clearParameters() throws SQLException {
        log.info("Clearing parameters");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        log.info("Setting object at index: {}, {}, {}", parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        log.info("Setting object at index: {}, {}", parameterIndex, x);
        changes.put(parameterIndex, x);
    }

    @Override
    public boolean execute() throws SQLException {
        log.info("Executing: {}", sql);
        return this.execute(sql);
    }

    @Override
    public void addBatch() throws SQLException {
        log.info("Adding batch");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        log.info("Setting character stream at index: {}", parameterIndex);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        log.info("Setting ref at index: {}", parameterIndex);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        log.info("Setting blob at index: {}", parameterIndex);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        log.info("Setting clob at index: {}", parameterIndex);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        log.info("Setting array at index: {}", parameterIndex);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        log.info("Getting metadata");
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        log.info("Setting date at index: {}", parameterIndex);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        log.info("Setting time at index: {}", parameterIndex);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        log.info("Setting timestamp at index: {}", parameterIndex);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        log.info("Setting null at index: {}", parameterIndex);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        log.info("Setting url at index: {}", parameterIndex);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        log.info("Getting parameter metadata");
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        log.info("Setting row id at index: {}", parameterIndex);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        log.info("Setting n string at index: {}", parameterIndex);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        log.info("Setting n character stream at index: {}", parameterIndex);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        log.info("Setting nclob at index: {}", parameterIndex);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        log.info("Setting clob at index: {}", parameterIndex);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        log.info("Setting blob at index: {}", parameterIndex);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        log.info("Setting nclob at index: {}", parameterIndex);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        log.info("Setting sql xml at index: {}", parameterIndex);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        log.info("Setting object at index: {}, {}, {}, {}", parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        log.info("Setting ascii stream at index: {}", parameterIndex);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        log.info("Setting binary stream at index: {}", parameterIndex);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        log.info("Setting character stream at index: {}", parameterIndex);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        log.info("Setting ascii stream at index: {}", parameterIndex);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        log.info("Setting binary stream at index: {}", parameterIndex);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        log.info("Setting character stream at index: {}", parameterIndex);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        log.info("Setting n character stream at index: {}", parameterIndex);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        log.info("Setting clob at index: {}", parameterIndex);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        log.info("Setting blob at index: {}", parameterIndex);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        log.info("Setting nclob at index: {}", parameterIndex);
    }

    private void executeCosmosSqlUpdate(String sql) throws SQLException {
        log.info("Transforming update sql: {}", sql);

        net.sf.jsqlparser.statement.Statement parsed = null;
        try {
            parsed = CCJSqlParserUtil.parse(sql, ccjSqlParser -> {
                ccjSqlParser.withUnsupportedStatements(true);
            });
        } catch (JSQLParserException e) {
            throw new SQLException(e);
        }

        if (parsed instanceof Update) {
            Update up = (Update) parsed;
            Expression where = up.getWhere();
            Table table = up.getTable();
            String tableName = table.getName();
            CosmosContainer container = connection.getCurrentDatabase().getContainer(tableName);
            //do not support partition other then id I guess
            //last object will be always part of LIKE as I do not know how to change dialect
            //also it will be always + 1 as jdbs starts count from 1
            Object likeId = changes.get(changes.size());

            SqlQuerySpec sqlQuerySpec = new SqlQuerySpec();
            sqlQuerySpec.setQueryText("Select * from c where c.id = @id");
            sqlQuerySpec.setParameters(new ArrayList<SqlParameter>() {{
                add(new SqlParameter("@id", likeId));
            }});
            CosmosQueryRequestOptions cosmosQueryRequestOptions = new CosmosQueryRequestOptions();
            CosmosPagedIterable<LinkedHashMap> query = container.queryItems(sqlQuerySpec, cosmosQueryRequestOptions, LinkedHashMap.class);
            Optional<LinkedHashMap> objectToUpdate = query.stream().findFirst();
            if (!objectToUpdate.isPresent()) {
                throw new SQLException("No object found for id: " + likeId);
            }

            List<UpdateSet> updateSets = up.getUpdateSets();

            LinkedHashMap entry = objectToUpdate.get();
            for (int i = 0; i < updateSets.size(); i++) {
                UpdateSet updateSet = updateSets.get(i);
                //NO arrays and only jdbc
                Integer index = ((JdbcParameter) updateSet.getValue(0)).getIndex();
                Object changingObject = changes.get(index);
                Column column = updateSet.getColumn(i);
                String columnName = column.getColumnName();
                entry.put(columnName, changingObject);
            }

            //TODO: should be get from metadata
            PartitionKey partitionKey = new PartitionKey(entry.get("id").toString());
            CosmosItemRequestOptions options = new CosmosItemRequestOptions();
            CosmosItemResponse<LinkedHashMap> tCosmosItemResponse = container
                    .upsertItem(entry, partitionKey, options);

            if (tCosmosItemResponse.getStatusCode() != 200) {
                throw new SQLException("Error while updating object: " + tCosmosItemResponse.getStatusCode());
            }
        }
    }
}
