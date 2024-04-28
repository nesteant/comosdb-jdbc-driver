package com.nesteant.cosmosjdbc.jdbc;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDatabase;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

@Slf4j
public class CosmosDbConnection implements Connection {

    protected CosmosClient client;

    protected final Map<String, CosmosDatabase> databases = new HashMap<>();

    protected final Map<String, String> connectionProps = new HashMap<>();
    protected CosmosDatabase currentDatabase;

    protected boolean isReadOnly = false;

    protected final Map<String, String> connectionAppInfo = new HashMap<>();

    static Connection initializeConnection(String url, Properties info) {
        return new CosmosDbConnection(url, info);
    }

    public CosmosDbConnection(String url, Properties info) {
        String connectionString = url.replace("jdbc:cosmosdb:", "");

        info.stringPropertyNames().forEach(key -> connectionProps.put(key.toUpperCase(), info.getProperty(key)));

        Arrays.stream(connectionString.split(";")).forEach(s -> {
            String[] split = s.split("=");
            if (split.length == 2) {
                connectionProps.put(split[0].toUpperCase(), split[1]);
            }
        });

        //TODO: parse url and info
        this.client = new CosmosClientBuilder()
                .endpoint(connectionProps.get("ACCOUNTENDPOINT"))
                .key(connectionProps.get("ACCOUNTKEY"))
                .buildClient();

        this.client.readAllDatabases().forEach(databaseProperties -> {
            databases.put(databaseProperties.getId(), client.getDatabase(databaseProperties.getId()));
        });
        currentDatabase = databases.values().stream().findAny().orElse(null);
        log.info("Connected to CosmosDB. Databases: {}", databases.keySet());
    }

    @Override
    public Statement createStatement() throws SQLException {
        log.info("Creating blank statement");
        return new CosmosDbStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        log.info("Creating prepared statement {}", sql);
        return new CosmosDbPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        log.info("Creating callable statement {}", sql);
        return new CosmosDbCallableStatement(this, sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        log.info("Native SQL: {}", sql);
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        log.info("Setting auto commit to {}", autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        log.info("Getting auto commit");
        return false;
    }

    @Override
    public void commit() throws SQLException {
        log.info("Committing transaction");
    }

    @Override
    public void rollback() throws SQLException {
        log.info("Rolling back transaction");
    }

    @Override
    public void close() throws SQLException {
        log.info("Closing connection");
        client.close();
        client = null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        log.info("Checking if connection is closed");
        return client == null;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        log.info("Getting cosmosdb metadata");
        return new CosmosDbDatabaseMetadata(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        log.info("Setting read only to {}", readOnly);
        isReadOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        log.info("Checking if read only");
        return isReadOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        log.info("Setting catalog to {}", catalog);
        currentDatabase = databases.get(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        String id = currentDatabase.getId();
        log.info("Getting catalog {}", id);
        return id;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        log.info("Setting transaction isolation to {}", level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        log.info("Getting transaction isolation");
        return Connection.TRANSACTION_SERIALIZABLE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
//        log.info("Getting warnings");
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
//        log.info("Clearing warnings");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        log.info("Creating statement with result set type {} and concurrency {}", resultSetType, resultSetConcurrency);
        return new CosmosDbStatement(this, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        log.info("Creating prepared statement {} with result set type {} and concurrency {}", sql, resultSetType, resultSetConcurrency);
        return new CosmosDbPreparedStatement(this, sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        log.info("Creating callable statement {} with result set type {} and concurrency {}", sql, resultSetType, resultSetConcurrency);
        return new CosmosDbCallableStatement(this, sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        log.info("Getting type map");
        return Collections.emptyMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        log.info("Setting type map {}", map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        log.info("Setting holdability to {}", holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        log.info("Getting holdability");
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        log.info("Setting savepoint");
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        log.info("Setting savepoint with name {}", name);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        log.info("Rolling back to savepoint {}", savepoint);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        log.info("Releasing savepoint {}", savepoint);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        log.info("Creating statement with result set type {}, concurrency {} and holdability {}", resultSetType, resultSetConcurrency, resultSetHoldability);
        return new CosmosDbStatement(this, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        log.info("Creating prepared statement {} with result set type {}, concurrency {} and holdability {}", sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        return new CosmosDbPreparedStatement(this, sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        log.info("Creating callable statement {} with result set type {}, concurrency {} and holdability {}", sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        return new CosmosDbCallableStatement(this, sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        log.info("Creating prepared statement {} with auto generated keys {}", sql, autoGeneratedKeys);
        //TODO: maybe needed
        return new CosmosDbPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        log.info("Creating prepared statement {} with column indexes {}", sql, columnIndexes);
        return new CosmosDbPreparedStatement(this, sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        log.info("Creating prepared statement {} with column names {}", sql, columnNames);
        return new CosmosDbPreparedStatement(this, sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        log.info("Creating Clob");
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        log.info("Creating Blob");
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        log.info("Creating NClob");
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        log.info("Creating SQLXML");
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        log.info("Checking if connection is valid with timeout {}", timeout);
        return client != null;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        log.info("Setting client info {} to {}", name, value);
        connectionAppInfo.put(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        log.info("Setting client info {}", properties);
        properties.stringPropertyNames().forEach(key -> connectionAppInfo.put(key, properties.getProperty(key)));
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        log.info("Getting client info {}", name);
        return connectionAppInfo.get(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        log.info("Getting client info");
        Properties properties = new Properties();
        properties.putAll(connectionAppInfo);
        return properties;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        log.info("Creating array of type {} with elements {}", typeName, elements);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        log.info("Creating struct of type {} with attributes {}", typeName, attributes);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        log.info("Setting schema to {}", schema);
//        CosmosDatabase db = databases.get(schema);
//        if (db == null) {
//            db = client.getDatabase(schema);
//            databases.put(schema, db);
//        }
    }

    @Override
    public String getSchema() throws SQLException {
        log.info("Getting schema");
//        return currentDatabase == null ? null : currentDatabase.getId();
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        log.info("Aborting connection");
        close();
        databases.clear();
        currentDatabase = null;
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        log.info("Setting network timeout to {} milliseconds", milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        log.info("Getting network timeout");
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        log.info("Unwrapping connection to {}", iface);
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        log.info("Checking if connection is wrapper for {}", iface);
        return iface.isInstance(this);
    }
}
