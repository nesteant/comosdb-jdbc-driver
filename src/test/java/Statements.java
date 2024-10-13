//import com.nesteant.cosmosjdbc.jdbc.CosmosDbConnection;
//import com.nesteant.cosmosjdbc.jdbc.CosmosDbDriver;
//import com.nesteant.cosmosjdbc.jdbc.CosmosDbStatement;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.sql.*;
//import java.util.Properties;
//
//public class Statements {
//
//    private static final String URL = System.getenv("COSMOS_URL");
//    private static final Logger log = LoggerFactory.getLogger(Statements.class);
//
//    private CosmosDbDriver cosmosDbDriver;
//
//    private Properties info = new Properties();
//
//    private Connection conn;
//
//    private String catalog;
//
//    private String defaultCatalog = System.getenv("COSMOS_CATALOG");
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
//    @Test
//    public void test() {
//        CosmosDbDriver cosmosDbDriver = new CosmosDbDriver();
//        Properties info = new Properties();
//        try (Connection conn = cosmosDbDriver.connect(URL, info);){
//            String catalog = conn.getCatalog();
//            String schema = conn.getSchema();
//            System.out.println("Catalog: " + catalog);
//            System.out.println("Schema: " + schema);
//
//            DatabaseMetaData metaData = conn.getMetaData();
//            System.out.println("Database Product Name: " + metaData.getDatabaseProductName());
//
//            ResultSet catalogs = metaData.getCatalogs();
//            while (catalogs.next()) {
//                System.out.println("Catalog: " + catalogs.getString(1));
//            }
//            ResultSet tableTypes = metaData.getTableTypes();
//            while (tableTypes.next()) {
//                System.out.println("Table Type: " + tableTypes.getString(1));
//            }
//
//
//            String x = "";
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @Test
//    public void test1() throws SQLException {
//        Statement statement = conn.createStatement();
//        boolean execute = statement.execute("SELECT t.* FROM containers.products t");
//
//        if (execute) {
//            ResultSet resultSet = statement.getResultSet();
//            while (resultSet.next()) {
//                System.out.println("Result: " + resultSet.getString(1));
//            }
//        }
//    }
//
//    @Test
//    public void prepareUpdate() throws SQLException {
//        conn.setCatalog(defaultCatalog);
//        PreparedStatement statement = conn.prepareStatement("UPDATE orders t SET t.externalOrderNumber = ? WHERE t.id LIKE ? ESCAPE '#'");
//        statement.setObject(1, "testtest");
//        statement.setObject(2, "56547131-1635-4045-87f3-6661a8b7eacb");
//        statement.executeUpdate();
//    }
//
//    @Test
//    public void test2() throws SQLException {
//        Statement statement1 = conn.createStatement();
//        boolean execute1 = statement1.execute("select * from pharmacies where pharmacies.test = 'test12'");
//        if (execute1) {
//            ResultSet resultSet = statement1.getResultSet();
//            while (resultSet.next()) {
//                System.out.println("Result: " + resultSet.getString(1));
//            }
//        }
//    }
//
//    @Test
//    public void test3() throws SQLException {
//        Statement statement1 = conn.createStatement();
//        boolean execute1 = statement1.execute("select top 10 * from pharmacies");
//        if (execute1) {
//            ResultSet resultSet = statement1.getResultSet();
//            while (resultSet.next()) {
//                System.out.println("Result: " + resultSet.getString(1));
//            }
//        }
//    }
//
//    @Test
//    public void testPrimaryKeys() throws SQLException {
//        ResultSet result = conn.getMetaData().getPrimaryKeys(catalog, schema, "pharmacies");
//
//        log.info("Primary keys: {}", result);
//    }
//
//    @Test
//    public void testBestRowIdentifier() throws SQLException {
//        ResultSet bestRowsResult = conn.getMetaData().getBestRowIdentifier(catalog, null, "pharmacies", 1, true);
//        log.info("Best Row Identifier: {}", bestRowsResult);
//    }
//
//    @Test
//    public void testColumns() throws SQLException {
//        ResultSet bestRowsResult = conn.getMetaData().getColumns(catalog, null, "pharmacies", null);
//        log.info("Best Row Identifier: {}", bestRowsResult);
//    }
//
//    @Test
//    public void testAttrs() throws SQLException {
//        Statement statement1 = conn.createStatement();
//        boolean execute1 = statement1.execute("select * from c where c.test = 'test12'");
//        if (execute1) {
//            ResultSet resultSet = statement1.getResultSet();
//            while (resultSet.next()) {
//                System.out.println("Result: " + resultSet.getString(1));
//            }
//        }
//    }
//
//    @Test
//    public void testAttrs1() throws SQLException {
//        conn.setCatalog(defaultCatalog);
//        CosmosDbStatement cosmosDbStatement = new CosmosDbStatement((CosmosDbConnection) conn);
//        ResultSet execute = cosmosDbStatement.executeQuery("select * from baskets");
//        String r = "";
//    }
//
//    @Test
//    public void testKeepAlive() throws SQLException {
//        conn.setCatalog(defaultCatalog);
//        CosmosDbStatement cosmosDbStatement = new CosmosDbStatement((CosmosDbConnection) conn);
//        boolean result = cosmosDbStatement.execute("SELECT 'keep alive'");
//        String r = "";
//    }
//}
