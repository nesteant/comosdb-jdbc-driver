package com.nesteant.cosmosjdbc.jdbc;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.gson.Gson;
import com.nesteant.cosmosjdbc.jdbc.models.CosmosRow;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;
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
        return false;
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
//        erb var1 = (new pob()).a(this.b, 1, "SELECT VALUE FROM sys_sqlinfo WHERE NAME = 'GROUP_BY'");
//        eib var2 = var1.a(var1.i());
//        var2.i();
//        String var3 = var2.d(0).toString();
//        return var3.equalsIgnoreCase("NO_RELATION");
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
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
//        erb var1 = (new pob()).a(this.b, 1, "SELECT VALUE FROM sys_sqlinfo WHERE NAME = 'OUTER_JOINS'");
//        eib var2 = var1.a(var1.i());
//        var2.i();
//        String var3 = var2.d(0).toString();
//        return var3.equalsIgnoreCase("YES");
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
//        erb var1 = (new pob()).a(this.b, 1, "SELECT VALUE FROM sys_sqlinfo WHERE NAME = 'OJ_CAPABILITIES'");
//        eib var2 = var1.a(var1.i());
//        var2.i();
//        String var3 = var2.d(0).toString();
//        return var3.equalsIgnoreCase("NESTED");
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return supportsFullOuterJoins();
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        //Not supported
        return "schema";
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
        return false;
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
        return CosmosDbResultSet.EMPTY(connection);
//        String var4;
//        if (this.b.aa() >= 5) {
//            var4 = "DatabaseMetaData.getProcedures(";
//            var4 = var4 + "catalog:" + (var1 == null ? "<null>" : "'" + var1 + "'");
//            var4 = var4 + ", schema:" + (var2 == null ? "<null>" : "'" + var2 + "'");
//            var4 = var4 + ", procedureNamePattern:" + (var3 == null ? "<null>" : "'" + var3 + "'");
//            var4 = var4 + ")";
//            this.b.a(5, sre.E, var4);
//        }
//
//        try {
//            var4 = "SELECT * FROM sys_procedures";
//            byte var5 = -1;
//            fjc[] var6 = ujc.o;
//            StringBuilder var7 = new StringBuilder();
//            var7.append("SELECT \n");
//            StringBuilder var10001 = (new StringBuilder()).append("CatalogName AS ");
//            int var15 = var5 + 1;
//            var7.append(var10001.append(this.a(var6, var15)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("SchemaName AS ");
//            ++var15;
//            var7.append(var10001.append(this.a(var6, var15)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("ProcedureName AS ");
//            ++var15;
//            var7.append(var10001.append(this.a(var6, var15)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("NULL AS ");
//            ++var15;
//            var7.append(var10001.append(this.a(var6, var15)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("NULL AS ");
//            ++var15;
//            var7.append(var10001.append(this.a(var6, var15)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("NULL AS ");
//            ++var15;
//            var7.append(var10001.append(this.a(var6, var15)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("Description AS ");
//            ++var15;
//            var7.append(var10001.append(this.a(var6, var15)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("CASE ProcedureType WHEN 'PROCEDURE' THEN 2 WHEN 'FUNCTION' THEN 1 ELSE 0 END AS ");
//            ++var15;
//            var7.append(var10001.append(this.a(var6, var15)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("ProcedureName AS ");
//            ++var15;
//            var7.append(var10001.append(this.a(var6, var15)).append("\n").toString());
//            var7.append("FROM \n");
//            var7.append("sys_procedures");
//            String var8 = var7.toString();
//            erb var9 = a(this.b, var4, new String[]{"CatalogName", "SchemaName", "ProcedureName"}, new String[]{var1, var2, var3});
//            eib var10 = var9.a(var9.i());
//            vmb var11 = mmb.a(var8).b();
//            dgc.b var12 = new dgc.b(ujc.o);
//            ldc var13 = this.a.createClassFactory();
//            return var13.createResultSet(this.a((vmb)var11, (eib)var10, (dac)null, (ijc)var12), this.a);
//        } catch (Exception var14) {
//            throw mgc.a(var14);
//        }
//        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        log.info("getProcedureColumns catalog: {}, schemaPattern: {}, procedureNamePattern: {}, columnNamePattern: {}", catalog, schemaPattern, procedureNamePattern, columnNamePattern);
        return CosmosDbResultSet.EMPTY(connection);

//        if (this.b.aa() >= 5) {
//            String var5 = "DatabaseMetaData.getProcedureColumns(";
//            var5 = var5 + "catalog:" + (var1 == null ? "<null>" : "'" + var1 + "'");
//            var5 = var5 + ", schemaPattern:" + (var2 == null ? "<null>" : "'" + var2 + "'");
//            var5 = var5 + ", procedureNamePattern:" + (var3 == null ? "<null>" : "'" + var3 + "'");
//            var5 = var5 + ", columnNamePattern:" + (var4 == null ? "<null>" : "'" + var4 + "'");
//            var5 = var5 + ")";
//            this.b.a(5, sre.E, var5);
//        }
//
//        try {
//            ntb var18 = new ntb();
//            String var6 = "SELECT * FROM sys_procedureparameters";
//            byte var7 = -1;
//            fjc[] var8 = ujc.p;
//            StringBuilder var9 = new StringBuilder();
//            var9.append("SELECT \n");
//            StringBuilder var10001 = (new StringBuilder()).append("CatalogName AS ");
//            int var19 = var7 + 1;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("SchemaName AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("ProcedureName AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("ColumnName AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("CAST(Direction AS SHORT) AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(", ").toString());
//            var10001 = (new StringBuilder()).append("DataType AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("DataTypeName AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("NumericPrecision AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("Length AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("CAST(NumericScale AS SHORT) AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("10 AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("CASE WHEN IsNullable = TRUE THEN '1' ELSE '0' END AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("Description AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("NULL AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("NULL AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("NULL AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("NULL AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("Ordinal AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("CASE WHEN IsNullable = TRUE THEN 'YES' ELSE 'NO' END AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("NULL AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("SupportsStreams AS ");
//            ++var19;
//            var9.append(var10001.append(this.a(var8, var19)).append(",\n").toString());
//            var9.append("FROM \n");
//            var9.append("sys_procedureparameters");
//            String var10 = var9.toString();
//            String var11 = null;
//            if (!bte.H(var3)) {
//                StringBuilder var12 = new StringBuilder();
//                bte.a(var3, var12);
//                var11 = var12.toString();
//            }
//
//            erb var20 = a(this.b, var6, new String[]{"CatalogName", "SchemaName", "ProcedureName", "ColumnName"}, new String[]{var1, var2, var11, var4});
//            eib var13 = var20.a(var20.i());
//            vmb var14 = mmb.a(var10).b();
//            dgc.b var15 = new dgc.b(ujc.p);
//            var18.a(this.a((vmb)var14, (eib)var13, (dac)null, (ijc)var15));
//            ldc var16 = this.a.createClassFactory();
//            return var16.createResultSet(var18, this.a);
//        } catch (Exception var17) {
//            throw mgc.a(var17);
//        }
//        return null;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        log.info("getTables catalog: {}, schemaPattern: {}, tableNamePattern: {}, types: {}", catalog, schemaPattern, tableNamePattern, types);

        CosmosClient client = connection.client;
        if (catalog == null) {
            log.info("getTables catalog is null");
            return CosmosDbResultSet.EMPTY(connection);
        }
        CosmosDatabase database = client.getDatabase(catalog);

        CosmosPagedIterable<CosmosContainerProperties> cosmosContainerProperties = database.readAllContainers();
        List<CosmosRow> rowList = cosmosContainerProperties.stream().map(container -> new CosmosRow(new LinkedHashMap<String, Object>() {
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
        })).collect(Collectors.toList());
        log.info("getTables rowList: {}", new Gson().toJson(rowList));

        return new CosmosDbResultSet(connection, rowList);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        log.info("getSchemas metadata");
        if (connection.currentDatabase == null) {
            log.info("getSchemas currentDatabase is null");
            return CosmosDbResultSet.EMPTY(connection);
        } else {
            log.info("getSchemas currentDatabase: {}", connection.currentDatabase.getId());
            return new CosmosDbResultSet(connection, new ArrayList<CosmosRow>() {{
                add(new CosmosRow(new LinkedHashMap<String, Object>() {{
                    put("TABLE_SCHEM", "containers");
                    put("TABLE_CATALOG", connection.currentDatabase.getId());
                }}));
            }});
        }
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        log.info("getCatalogs");
        Collection<CosmosDatabase> catalogs = connection.databases.values();

        List<CosmosRow> rowList = catalogs.stream().map(database -> new CosmosRow(new LinkedHashMap<String, Object>() {
            {
                put("TABLE_CAT", database.getId());
            }
        })).collect(Collectors.toList());
        log.info("getCatalogs rowList: {}", rowList);
        return new CosmosDbResultSet(connection, rowList);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        log.info("getTableTypes");
        List<CosmosRow> results = new ArrayList<CosmosRow>() {{
            add(new CosmosRow(new LinkedHashMap<String, Object>() {{
                put("TABLE_TYPE", "TABLE");
            }}));
        }};
        log.info("getTableTypes results: {}", new Gson().toJson(results));
        return new CosmosDbResultSet(connection, results);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        log.info("getColumns catalog: {}, schemaPattern: {}, tableNamePattern: {}, columnNamePattern: {}", catalog, schemaPattern, tableNamePattern, columnNamePattern);

        try {
            CosmosDatabase cosmosDatabase = connection.databases.get(catalog);

            CosmosPagedIterable<CosmosContainerProperties> cosmosContainerProperties = cosmosDatabase.readAllContainers();
            List<CosmosRow> rowList = new ArrayList<>();

            cosmosContainerProperties.forEach(container -> {
                log.info("getColumns container: {}", container.getId());
                CosmosPagedIterable<Object> objects = cosmosDatabase.getContainer(container.getId()).queryItems("select top 40 * from " + container.getId(), null, Object.class);
                List<CosmosRow> rows = objects.stream().map(CosmosRow::new).collect(Collectors.toList());
                List<String> columns = new ArrayList<>();
                rows.forEach(row -> row.getParsed().forEach(pair -> {
                    if (!columns.contains(pair.getLeft())) {
                        columns.add(pair.getLeft());
                    }
                }));
                rows.forEach(row -> row.setColumns(columns));

                if (!rows.isEmpty()) {
                    CosmosRow cosmosRow = rows.get(0);
                    cosmosRow.getColumns().forEach(columnName -> {
                        rowList.add(new CosmosRow(new LinkedHashMap<String, Object>() {{
                            put("TABLE_CAT", catalog);
                            put("TABLE_SCHEM", null);
                            put("TABLE_NAME", container.getId());
                            put("COLUMN_NAME", columnName);
                            put("DATA_TYPE", Types.VARCHAR);
                            put("TYPE_NAME", "VARCHAR");
                            put("COLUMN_SIZE", 255);
                            put("BUFFER_LENGTH", 255);
                            put("DECIMAL_DIGITS", 0);
                            put("NUM_PREC_RADIX", 10);
                            put("NULLABLE", 0);
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
                        }}));
                    });
                }
            });
            return new CosmosDbResultSet(connection, rowList);
        } catch (Exception e) {
            log.error("getColumns error", e);
            return CosmosDbResultSet.EMPTY(connection);
        }
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        log.info("getColumnPrivileges catalog: {}, schema: {}, table: {}, columnNamePattern: {}", catalog, schema, table, columnNamePattern);
        return CosmosDbResultSet.EMPTY(connection);
//        if (this.b.aa() >= 5) {
//            String var5 = "DatabaseMetaData.getColumnPrivileges(";
//            var5 = var5 + "catalog:" + (var1 == null ? "<null>" : "'" + var1 + "'");
//            var5 = var5 + ", schema:" + (var2 == null ? "<null>" : "'" + var2 + "'");
//            var5 = var5 + ", table:" + (var3 == null ? "<null>" : "'" + var3 + "'");
//            var5 = var5 + ", columnNamePattern:" + (var4 == null ? "<null>" : "'" + var4 + "'");
//            var5 = var5 + ")";
//            this.b.a(5, sre.E, var5);
//        }
//
//        try {
//            Vector var9 = new Vector();
//            if (this.a(var1) && this.b(var2) && !bte.H(var3) && !bte.H(var4)) {
//                var9.add(a((Object[])(new String[]{var1, var2, var3, var4, null, null, null, null})));
//            }
//
//            eib var6 = this.a(ujc.c, var9);
//            ldc var7 = this.a.createClassFactory();
//            return var7.createResultSet(var6, this.a);
//        } catch (Exception var8) {
//            throw mgc.a(var8);
//        }
//        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        log.info("getTablePrivileges catalog: {}, schemaPattern: {}, tableNamePattern: {}", catalog, schemaPattern, tableNamePattern);
        return CosmosDbResultSet.EMPTY(connection);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        log.info("getBestRowIdentifier catalog: {}, schema: {}, table: {}, scope: {}, nullable: {}", catalog, schema, table, scope, nullable);
        CosmosContainerResponse read = connection.databases.get(catalog).getContainer(table).read();

        List<CosmosRow> rowList = new ArrayList<CosmosRow>() {{
            add(new CosmosRow(new LinkedHashMap<String, Object>() {{
                put("SCOPE", scope);
                put("COLUMN_NAME", "id");
                put("DATA_TYPE", Types.VARCHAR);
                put("TYPE_NAME", "VARCHAR");
                put("COLUMN_SIZE", 255);
                put("BUFFER_LENGTH", 255);
                put("DECIMAL_DIGITS", 0);
                put("PSEUDO_COLUMN", 1);
            }}));
        }};

        return new CosmosDbResultSet(connection, rowList);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        log.info("getVersionColumns catalog: {}, schema: {}, table: {}", catalog, schema, table);
        return CosmosDbResultSet.EMPTY(connection);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        log.info("getPrimaryKeys catalog: {}, schema: {}, table: {}", catalog, schema, table);
        CosmosContainerResponse read = connection.databases.get(catalog).getContainer(table).read();
        List<CosmosRow> rowList = new ArrayList<CosmosRow>() {{
            add(new CosmosRow(new LinkedHashMap<String, Object>() {{
                put("TABLE_CAT", catalog);
                put("TABLE_SCHEM", null);
                put("TABLE_NAME", table);
                put("COLUMN_NAME", "id");
                put("KEY_SEQ", 1);
                put("PK_NAME", "id");
            }}));
        }};

        return new CosmosDbResultSet(connection, rowList);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        log.info("getImportedKeys catalog: {}, schema: {}, table: {}", catalog, schema, table);
        return CosmosDbResultSet.EMPTY(connection);
//        if (this.b.aa() >= 5) {
//            String var4 = "DatabaseMetaData.getImportedKeys(";
//            var4 = var4 + "catalog:" + (var1 == null ? "<null>" : "'" + var1 + "'");
//            var4 = var4 + ", schema:" + (var2 == null ? "<null>" : "'" + var2 + "'");
//            var4 = var4 + ", table:" + (var3 == null ? "<null>" : "'" + var3 + "'");
//            var4 = var4 + ")";
//            this.b.a(5, sre.E, var4);
//        }
//
//        try {
//            Vector var8 = this.a(var1, var2, var3);
//            eib var5 = this.a(ujc.n, var8);
//            ldc var6 = this.a.createClassFactory();
//            return var6.createResultSet(var5, this.a);
//        } catch (Exception var7) {
//            throw mgc.a(var7);
//        }
//        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        log.info("getExportedKeys catalog: {}, schema: {}, table: {}", catalog, schema, table);
        return CosmosDbResultSet.EMPTY(connection);
//        if (this.b.aa() >= 5) {
//            String var4 = "DatabaseMetaData.getExportedKeys(";
//            var4 = var4 + "catalog:" + (var1 == null ? "<null>" : "'" + var1 + "'");
//            var4 = var4 + ", schema:" + (var2 == null ? "<null>" : "'" + var2 + "'");
//            var4 = var4 + ", table:" + (var3 == null ? "<null>" : "'" + var3 + "'");
//            var4 = var4 + ")";
//            this.b.a(5, sre.E, var4);
//        }
//
//        try {
//            Vector var8 = this.b(var1, var2, var3);
//            eib var5 = this.a(ujc.n, var8);
//            ldc var6 = this.a.createClassFactory();
//            return var6.createResultSet(var5, this.a);
//        } catch (Exception var7) {
//            throw mgc.a(var7);
//        }
//        return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        log.info("getCrossReference parentCatalog: {}, parentSchema: {}, parentTable: {}, foreignCatalog: {}, foreignSchema: {}, foreignTable: {}", parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);
        return CosmosDbResultSet.EMPTY(connection);
//        if (this.b.aa() >= 5) {
//            String var7 = "DatabaseMetaData.getCrossReference(";
//            var7 = var7 + "primaryCatalog:" + (var1 == null ? "<null>" : "'" + var1 + "'");
//            var7 = var7 + ", primarySchema:" + (var2 == null ? "<null>" : "'" + var2 + "'");
//            var7 = var7 + ", primaryTable:" + (var3 == null ? "<null>" : "'" + var3 + "'");
//            var7 = var7 + ", foreignCatalog:" + (var4 == null ? "<null>" : "'" + var4 + "'");
//            var7 = var7 + ", foreignSchema:" + (var5 == null ? "<null>" : "'" + var5 + "'");
//            var7 = var7 + ", foreignTable:" + (var6 == null ? "<null>" : "'" + var6 + "'");
//            var7 = var7 + ")";
//            this.b.a(5, sre.E, var7);
//        }
//
//        try {
//            Vector var11 = new Vector();
//            if (this.a(var1) && this.a(var4) && this.b(var2) && this.b(var5)) {
//                var11.add(a(new Object[]{var1, var2, var3, null, var4, var5, var6, null, Short.valueOf((short)1), Short.valueOf((short)3), Short.valueOf((short)3), null, null, Short.valueOf((short)7)}));
//            }
//
//            eib var8 = this.a(ujc.h, var11);
//            ldc var9 = this.a.createClassFactory();
//            return var9.createResultSet(var8, this.a);
//        } catch (Exception var10) {
//            throw mgc.a(var10);
//        }
//        return null;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        log.info("getTypeInfo");
        return CosmosDbResultSet.EMPTY(connection);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        log.info("getIndexInfo catalog: {}, schema: {}, table: {}, unique: {}, approximate: {}", catalog, schema, table, unique, approximate);
        return CosmosDbResultSet.EMPTY(connection);
//        String var6;
//        if (this.b.aa() >= 5) {
//            var6 = "DatabaseMetaData.getIndexInfo(";
//            var6 = var6 + "catalog:" + (var1 == null ? "<null>" : "'" + var1 + "'");
//            var6 = var6 + ", schema:" + (var2 == null ? "<null>" : "'" + var2 + "'");
//            var6 = var6 + ", table:" + (var3 == null ? "<null>" : "'" + var3 + "'");
//            var6 = var6 + ")";
//            this.b.a(5, sre.E, var6);
//        }
//
//        try {
//            var6 = "SELECT * FROM sys_indexes";
//            String[] var7 = new String[]{"CatalogName", "SchemaName", "TableName"};
//            String[] var8 = new String[]{var1, var2, var3};
//            erb var9 = a(this.b, var6, var7, var8);
//            eib var10 = var9.a(var9.i());
//            byte var11 = -1;
//            fjc[] var12 = ujc.j;
//            StringBuilder var13 = new StringBuilder();
//            var13.append("SELECT \n");
//            StringBuilder var10001 = (new StringBuilder()).append("CatalogName AS ");
//            int var17 = var11 + 1;
//            var13.append(var10001.append(this.a(var12, var17)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("SchemaName AS ");
//            ++var17;
//            var13.append(var10001.append(this.a(var12, var17)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("TableName AS ");
//            ++var17;
//            var13.append(var10001.append(this.a(var12, var17)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("CASE IsUnique WHEN true THEN false ELSE true END AS ");
//            ++var17;
//            var13.append(var10001.append(this.a(var12, var17)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("'' AS ");
//            ++var17;
//            var13.append(var10001.append(this.a(var12, var17)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("IndexName AS ");
//            ++var17;
//            var13.append(var10001.append(this.a(var12, var17)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("Type AS ");
//            ++var17;
//            var13.append(var10001.append(this.a(var12, var17)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("CAST(EXPR(Ordinal_Position + 1) AS SHORT) AS ");
//            ++var17;
//            var13.append(var10001.append(this.a(var12, var17)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("ColumnName AS ");
//            ++var17;
//            var13.append(var10001.append(this.a(var12, var17)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("SortOrder AS ");
//            ++var17;
//            var13.append(var10001.append(this.a(var12, var17)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("1 AS ");
//            ++var17;
//            var13.append(var10001.append(this.a(var12, var17)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("0 AS ");
//            ++var17;
//            var13.append(var10001.append(this.a(var12, var17)).append(",\n").toString());
//            var10001 = (new StringBuilder()).append("NULL AS ");
//            ++var17;
//            var13.append(var10001.append(this.a(var12, var17)).append("\n").toString());
//            var13.append("FROM \n");
//            var13.append("sys_indexes");
//            pmb var14 = mmb.a(var13.toString()).g();
//            dgc.b var15 = new dgc.b(ujc.j);
//            return this.a.createClassFactory().createResultSet(this.a((vmb)var14, (eib)var10, (dac)null, (ijc)var15), this.a);
//        } catch (Exception var16) {
//            throw mgc.a(var16);
//        }
//        return null;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        log.info("getUDTs catalog: {}, schemaPattern: {}, typeNamePattern: {}, types: {}", catalog, schemaPattern, typeNamePattern, types);
        return CosmosDbResultSet.EMPTY(connection);
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
        return CosmosDbResultSet.EMPTY(connection);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        log.info("getSuperTables catalog: {}, schemaPattern: {}, tableNamePattern: {}", catalog, schemaPattern, tableNamePattern);
        return CosmosDbResultSet.EMPTY(connection);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        log.info("getAttributes catalog: {}, schemaPattern: {}, typeNamePattern: {}, attributeNamePattern: {}", catalog, schemaPattern, typeNamePattern, attributeNamePattern);
        return CosmosDbResultSet.EMPTY(connection);
//        if (this.b.aa() >= 5) {
//            String var5 = "DatabaseMetaData.getAttributes(";
//            var5 = var5 + "catalog:" + (var1 == null ? "<null>" : "'" + var1 + "'");
//            var5 = var5 + ", schemaPattern:" + (var2 == null ? "<null>" : "'" + var2 + "'");
//            var5 = var5 + ", typeNamePattern:" + (var3 == null ? "<null>" : "'" + var3 + "'");
//            var5 = var5 + ", attributeNamePattern:" + (var4 == null ? "<null>" : "'" + var4 + "'");
//            var5 = var5 + ")";
//            this.b.a(5, sre.E, var5);
//        }
//
//        try {
//            Vector var10 = new Vector();
//            if (this.a(var1) && this.b(var2)) {
//                Vector var6 = a(new Object[]{var1, var2, var3, var4, 12, null, null, 5, 10, 2, null, null, null, null, 1000, 1, "", null, null, null, null});
//                var10.add(var6);
//            }
//
//            eib var9 = this.a(ujc.d, var10);
//            ldc var7 = this.a.createClassFactory();
//            return var7.createResultSet(var9, this.a);
//        } catch (Exception var8) {
//            throw mgc.a(var8);
//        }
//        return null;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        log.info("getSchemas catalog: {}, schemaPattern: {}", catalog, schemaPattern);
        return new CosmosDbResultSet(connection, new ArrayList<CosmosRow>() {{
            add(new CosmosRow(new LinkedHashMap<String, Object>() {{
                put("TABLE_SCHEM", "containers");
                put("TABLE_CATALOG", catalog);
            }}));
        }});
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        log.info("getClientInfoProperties");
//        if (this.b.aa() >= 5) {
//            String var1 = "DatabaseMetaData.getClientInfoProperties()";
//            this.b.a(5, sre.E, var1);
//        }
//
//        try {
//            Vector var5 = new Vector();
//            eib var2 = this.a(ujc.g, var5);
//            ldc var3 = this.a.createClassFactory();
//            return var3.createResultSet(var2, this.a);
//        } catch (Exception var4) {
//            throw mgc.a(var4);
//        }
        return CosmosDbResultSet.EMPTY(connection);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        log.warn("getPseudoColumns not supported");
        return CosmosDbResultSet.EMPTY(connection);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        log.warn("getPseudoColumns not supported");
        return CosmosDbResultSet.EMPTY(connection);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        log.warn("getPseudoColumns not supported");
        return CosmosDbResultSet.EMPTY(connection);
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
