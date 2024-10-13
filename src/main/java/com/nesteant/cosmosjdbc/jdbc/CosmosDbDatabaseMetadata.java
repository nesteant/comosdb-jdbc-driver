package com.nesteant.cosmosjdbc.jdbc;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.gson.Gson;
import com.nesteant.cosmosjdbc.jdbc.models.CosmosSqlColumn;
import com.nesteant.cosmosjdbc.jdbc.resultset.AsyncCosmosSqlResultSet;
import com.nesteant.cosmosjdbc.jdbc.resultset.SimpleCosmosSqlResultSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableInt;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class CosmosDbDatabaseMetadata implements DatabaseMetaData {

    private CosmosDbConnection connection;

    public CosmosDbDatabaseMetadata(CosmosDbConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        log.info("allProceduresAreCallable");
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        log.info("allTablesAreSelectable");
        return false;
    }

    @Override
    public String getURL() throws SQLException {
        String endpoint = this.connection.connectionProps.get("ACCOUNTENDPOINT");
        log.info("getURL: {}", endpoint);
        return endpoint;
    }

    @Override
    public String getUserName() throws SQLException {
        log.info("getUserName");
        return "CosmosDB";
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        log.info("isReadOnly");
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        log.info("nullsAreSortedHigh");
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        log.info("nullsAreSortedLow");
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        log.info("nullsAreSortedAtStart");
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        log.info("nullsAreSortedAtEnd");
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        log.info("getDatabaseProductName");
        return "CosmosDB";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "1.0";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "cosmosdb";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "1.0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 1;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "ALTER, AND, ANY, ASC, AUTO_INCREMENT, AUTOINCREMENT, BETWEEN, BY, CACHE, CALL, CASE, CHECKCACHE, CREATE, COLLATE, COMMIT, CONSTRAINT, CROSS, CURRENT, CURRENT_DATE, CURRENT_TIMESTAMP, CURRENT_TIME, DEFAULT, DELETE, DESC, DISTINCT, DROP, EACH, ELSE, END, EXCEPT, EXISTS, FIRST, FOLLOWING, FROM, FALSE, GETDELETED, GROUP, HAVING, IF, INNER, INSERT, INTERSECT, IS, IN, INNER, INTERSECT, INTERVAL, INTO, JOIN, KILL, LAST, LIMIT, LIKE, LOADMEMORYQUERY, MERGE, METAQUERY, MINUS, NOT, NATURAL, NULL, NULLS, ON, OFFSET, OR, ORDER, OUTER, OUTPUT, OVER, PARTITION, PRECEDING, PRIMARY, RANGE, REPLICATE, RESET, ROWS, SELECT, SOME, THEN, TRUE, UNBOUNDED, UNION, UNIQUE, UPSERT, VALUES, WHEN, WHERE, WITH";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
//        erb var1 = (new pob()).a(this.b, 1, "SELECT VALUE FROM sys_sqlinfo WHERE NAME = 'NUMERIC_FUNCTIONS'");
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
//        erb var1 = (new pob()).a(this.b, 1, "SELECT VALUE FROM sys_sqlinfo WHERE NAME = 'STRING_FUNCTIONS'");
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
//        erb var1 = (new pob()).a(this.b, 1, "SELECT VALUE FROM sys_sqlinfo WHERE NAME = 'TIMEDATE_FUNCTIONS'");
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return true;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return supportsGroupByUnrelated();
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return supportsFullOuterJoins();
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        //Not supported
        return "container";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "database";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        //TODO
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 128;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 128;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        //TODO
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        //TODO
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        //TODO
        return true;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY == type;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY == type && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        //TODO
        return false;
    }


    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        log.info("getProcedures catalog: {}, schemaPattern: {}, procedureNamePattern: {}", catalog, schemaPattern, procedureNamePattern);
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        log.info("getProcedureColumns catalog: {}, schemaPattern: {}, procedureNamePattern: {}, columnNamePattern: {}", catalog, schemaPattern, procedureNamePattern, columnNamePattern);
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        log.info("getTables catalog: {}, schemaPattern: {}, tableNamePattern: {}, types: {}", catalog, schemaPattern, tableNamePattern, types);

        CosmosClient client = connection.client;
        if (catalog == null) {
            log.info("getTables catalog is null");
            return new SimpleCosmosSqlResultSet(connection);
        }
        CosmosDatabase database = client.getDatabase(catalog);

        CosmosPagedIterable<CosmosContainerProperties> cosmosContainerProperties = database.readAllContainers();
        List<LinkedHashMap> rowList = cosmosContainerProperties.stream().map(container -> new LinkedHashMap<String, Object>() {
            {
                {
                    put("TABLE_CAT", catalog);
                    put("TABLE_SCHEM", null);
                    put("TABLE_NAME", container.getId());
                    put("TABLE_TYPE", "TABLE");
                    put("REMARKS", null);
                    put("TYPE_CAT", null);
                    put("TYPE_SCHEM", null);
                    put("TYPE_NAME", null);
                    put("SELF_REFERENCING_COL_NAME", null);
                    put("REF_GENERATION", null);
                }
            }
        }).collect(Collectors.toList());
        log.info("getTables rowList: {}", new Gson().toJson(rowList));

        return new SimpleCosmosSqlResultSet(connection, rowList);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        log.info("getSchemas metadata");
        log.info("getSchemas currentDatabase: {}", connection.currentDatabase);
        if (connection.currentDatabase == null || connection.currentDatabase.getId() == null) {
            log.info("getSchemas currentDatabase is null");
            return new SimpleCosmosSqlResultSet(connection);
        } else {
            String id = connection.currentDatabase.getId();
            log.info("getSchemas currentDatabase: {}", id);
            ArrayList<LinkedHashMap> objects = new ArrayList<LinkedHashMap>() {{
                add(new LinkedHashMap<String, Object>() {{
                    put("TABLE_SCHEM", "containers");
                    put("TABLE_CATALOG", id);
                }});
            }};
            log.info("getSchemas objects: {}", objects);
            return new SimpleCosmosSqlResultSet(connection, objects);
        }
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        log.info("getCatalogs");
        Collection<CosmosDatabase> catalogs = connection.databases.values();

        List<LinkedHashMap> rowList = catalogs.stream().map(database -> new LinkedHashMap<String, Object>() {
            {
                put("TABLE_CAT", database.getId());
            }
        }).collect(Collectors.toList());
        log.info("getCatalogs rowList: {}", rowList);
        return new SimpleCosmosSqlResultSet(connection, rowList);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        log.info("getTableTypes");
        ArrayList<LinkedHashMap> results = new ArrayList<LinkedHashMap>() {{
            add(new LinkedHashMap<String, Object>() {{
                put("TABLE_TYPE", "TABLE");
            }});
        }};
        log.info("getTableTypes results: {}", results);
        return new SimpleCosmosSqlResultSet(connection, results);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        log.info("getColumns catalog: {}, schemaPattern: {}, tableNamePattern: {}, columnNamePattern: {}", catalog, schemaPattern, tableNamePattern, columnNamePattern);

        try {
            CosmosDatabase cosmosDatabase = connection.databases.get(catalog);
            log.info("getColumns cosmosDatabase: {}", cosmosDatabase.getId());
            CosmosPagedIterable<CosmosContainerProperties> cosmosContainerProperties = cosmosDatabase.readAllContainers();
            List<LinkedHashMap> rowList = new ArrayList<>();

            log.info("getColumns cosmosContainerProperties: {}", cosmosContainerProperties.stream().count());
            cosmosContainerProperties.stream().forEach(container -> {
                log.info("getColumns container: {}", container.getId());
                CosmosPagedIterable<LinkedHashMap> objects = cosmosDatabase.getContainer(container.getId()).queryItems("select top 40 * from c", null, LinkedHashMap.class);
                AsyncCosmosSqlResultSet result = AsyncCosmosSqlResultSet.create(connection, objects, 100);
                result.fetchAll();
                List<CosmosSqlColumn> columns = result.getColumns();

                columns.forEach(column -> {
                    rowList.add(new LinkedHashMap<String, Object>() {{
                        put("TABLE_CAT", catalog);
                        put("TABLE_SCHEM", null);
                        put("TABLE_NAME", container.getId());
                        put("COLUMN_NAME", column.name);
                        put("DATA_TYPE", column.getType());
                        put("TYPE_NAME", JDBCType.valueOf(column.getType()).getName());
//                        put("COLUMN_SIZE", 255);
                        put("COLUMN_SIZE", null);
//                        put("BUFFER_LENGTH", 255);
                        put("DECIMAL_DIGITS", 0);
                        put("NUM_PREC_RADIX", 10);
                        put("NULLABLE", 1);
                        put("REMARKS", null);
                        put("COLUMN_DEF", null);
                        put("SQL_DATA_TYPE", 0);
                        put("SQL_DATETIME_SUB", 0);
                        put("CHAR_OCTET_LENGTH", 0);
                        put("ORDINAL_POSITION", 1);
                        put("IS_NULLABLE", "YES");
                        put("SCOPE_CATALOG", null);
                        put("SCOPE_SCHEMA", null);
                        put("SCOPE_TABLE", null);
                        put("SOURCE_DATA_TYPE", null);
                        put("IS_AUTOINCREMENT", "NO");
                        put("IS_GENERATEDCOLUMN", "NO");
                    }});
                });
            });
            log.info("found columns: {}", rowList.size());
            return new SimpleCosmosSqlResultSet(connection, rowList);
        } catch (Exception e) {
            log.error("getColumns error", e);
            return new SimpleCosmosSqlResultSet(connection);
        }
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        log.info("getColumnPrivileges catalog: {}, schema: {}, table: {}, columnNamePattern: {}", catalog, schema, table, columnNamePattern);
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        log.info("getTablePrivileges catalog: {}, schemaPattern: {}, tableNamePattern: {}", catalog, schemaPattern, tableNamePattern);
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        log.info("getBestRowIdentifier catalog: {}, schema: {}, table: {}, scope: {}, nullable: {}", catalog, schema, table, scope, nullable);
        CosmosContainerResponse read = connection.databases.get(catalog).getContainer(table).read();

        List<LinkedHashMap> rowList = new ArrayList<LinkedHashMap>() {{
            add(new LinkedHashMap<String, Object>() {{
                put("SCOPE", scope);
                put("COLUMN_NAME", "id");
                put("DATA_TYPE", Types.VARCHAR);
                put("TYPE_NAME", "VARCHAR");
                put("COLUMN_SIZE", 255);
                put("BUFFER_LENGTH", 255);
                put("DECIMAL_DIGITS", 0);
                put("PSEUDO_COLUMN", 0);
            }});
        }};

        return new SimpleCosmosSqlResultSet(connection, rowList);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        log.info("getVersionColumns catalog: {}, schema: {}, table: {}", catalog, schema, table);
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        log.info("getPrimaryKeys catalog: {}, schema: {}, table: {}", catalog, schema, table);
        CosmosContainerResponse read = connection.databases.get(catalog).getContainer(table).read();

        List<String> paths = read.getProperties().getPartitionKeyDefinition().getPaths();

        MutableInt seq = new MutableInt(1);
        List<LinkedHashMap> additionalIndices = paths.stream()
                .map(path -> path.substring(1))
                .map(path -> path.split("/")[0])
                .map((path) -> {
                    seq.increment();
                    return new LinkedHashMap() {{
                        put("TABLE_CAT", catalog);
                        put("TABLE_SCHEM", null);
                        put("TABLE_NAME", table);
                        put("COLUMN_NAME", path);
                        put("KEY_SEQ", seq.intValue());
                        put("PK_NAME", read.getProperties().getId());
                    }};
                })
                .collect(Collectors.toList());
        log.info("getPrimaryKeys paths: {}", paths);
        List<LinkedHashMap> rowList = new ArrayList<LinkedHashMap>() {{
            add(new LinkedHashMap<String, Object>() {{
                put("TABLE_CAT", catalog);
                put("TABLE_SCHEM", null);
                put("TABLE_NAME", table);
                put("COLUMN_NAME", read.getProperties().getId());
                put("KEY_SEQ", 1);
                put("PK_NAME", read.getProperties().getId());
            }});
        }};

        rowList.addAll(additionalIndices);

        return new SimpleCosmosSqlResultSet(connection, rowList);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        log.info("getImportedKeys catalog: {}, schema: {}, table: {}", catalog, schema, table);
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        log.info("getExportedKeys catalog: {}, schema: {}, table: {}", catalog, schema, table);
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        log.info("getCrossReference parentCatalog: {}, parentSchema: {}, parentTable: {}, foreignCatalog: {}, foreignSchema: {}, foreignTable: {}", parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        log.info("getTypeInfo");
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        log.info("getIndexInfo catalog: {}, schema: {}, table: {}, unique: {}, approximate: {}", catalog, schema, table, unique, approximate);
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        log.info("getUDTs catalog: {}, schemaPattern: {}, typeNamePattern: {}, types: {}", catalog, schemaPattern, typeNamePattern, types);
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 2;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        //TODO
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 2;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return DatabaseMetaData.sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        log.info("getSuperTypes catalog: {}, schemaPattern: {}, typeNamePattern: {}", catalog, schemaPattern, typeNamePattern);
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        log.info("getSuperTables catalog: {}, schemaPattern: {}, tableNamePattern: {}", catalog, schemaPattern, tableNamePattern);
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        log.info("getAttributes catalog: {}, schemaPattern: {}, typeNamePattern: {}, attributeNamePattern: {}", catalog, schemaPattern, typeNamePattern, attributeNamePattern);
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        log.info("getSchemas catalog: {}, schemaPattern: {}", catalog, schemaPattern);
        return new SimpleCosmosSqlResultSet(connection, new ArrayList<LinkedHashMap>() {{
            add(new LinkedHashMap<String, Object>() {{
                put("TABLE_SCHEM", "containers");
                put("TABLE_CATALOG", catalog);
            }});
        }});
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        log.info("getClientInfoProperties");
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        log.warn("getPseudoColumns not supported");
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        log.warn("getPseudoColumns not supported");
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        log.warn("getPseudoColumns not supported");
        return new SimpleCosmosSqlResultSet(connection);
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
