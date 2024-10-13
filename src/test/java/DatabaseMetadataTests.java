//import com.nesteant.cosmosjdbc.jdbc.CosmosDbConnection;
//import com.nesteant.cosmosjdbc.jdbc.CosmosDbDatabaseMetadata;
//import com.nesteant.cosmosjdbc.jdbc.CosmosDbDriver;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
//import java.sql.SQLException;
//import java.util.Properties;
//
//public class DatabaseMetadataTests {
//
//    private static final String URL = System.getenv("COSMOS_URL");
//    private static final Logger log = LoggerFactory.getLogger(Statements.class);
//    private String defaultCatalog = System.getenv("COSMOS_CATALOG");
//
//    private CosmosDbDriver cosmosDbDriver;
//
//    private Properties info = new Properties();
//
//    private Connection conn;
//
//    private String catalog;
//
//    private String schema;
//
//    @Before
//    public void setUp() throws SQLException {
//        cosmosDbDriver = new CosmosDbDriver();
//        conn = cosmosDbDriver.connect(URL, info);
//        catalog = conn.getCatalog();
//        schema = conn.getSchema();
//    }
//
//    @After
//    public void tearDown() {
//        if (conn != null) {
//            try {
//                conn.close();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }
//
//
//    @Test
//    public void testPrimaryKeys() throws SQLException {
//        CosmosDbDatabaseMetadata metadata = new CosmosDbDatabaseMetadata((CosmosDbConnection) conn);
//        ResultSet tables = metadata.getPrimaryKeys(defaultCatalog, "", "tt");
//    }
//
//    @Test
//    public void testGetTables() throws SQLException {
//        CosmosDbDatabaseMetadata metadata = new CosmosDbDatabaseMetadata((CosmosDbConnection) conn);
//        ResultSet tables = metadata.getTables(defaultCatalog, "", "", null);
//    }
//
//    @Test
//    public void test() throws SQLException {
//        CosmosDbDatabaseMetadata metadata = new CosmosDbDatabaseMetadata((CosmosDbConnection) conn);
//
//        ResultSet schemas = metadata.getSchemas(defaultCatalog, "%");
//        ResultSetMetaData metaData = schemas.getMetaData();
//        String columnName = metaData.getColumnName(1);
//        ResultSet columns = metadata.getColumns(defaultCatalog, "", "", "");
//    }
//}
