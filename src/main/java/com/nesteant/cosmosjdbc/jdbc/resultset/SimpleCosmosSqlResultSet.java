package com.nesteant.cosmosjdbc.jdbc.resultset;

import com.nesteant.cosmosjdbc.jdbc.CosmosDbConnection;
import com.nesteant.cosmosjdbc.jdbc.metadata.CosmosSqlResultSetMetadata;
import com.nesteant.cosmosjdbc.jdbc.models.CosmosSqlColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;

public class SimpleCosmosSqlResultSet implements CosmosSqlResultSet {

    protected static final Logger log = LoggerFactory.getLogger(SimpleCosmosSqlResultSet.class);
    protected CosmosDbConnection connection;
    private List<LinkedHashMap> localItems = new ArrayList<>();
    private int fetchSize = 20;
    private int currentRowIndex = -1;
    private LinkedHashMap<String, CosmosSqlColumn> columnsMap = new LinkedHashMap<>();
    private LinkedHashMap<Integer, CosmosSqlColumn> columnsIndexMap = new LinkedHashMap<>();

    public int getColumnCount() {
        return columnsMap.size();
    }

    public String getColumnName(int column) {
        log.info("getColumnName {}", column);
        CosmosSqlColumn cosmosSqlColumn = columnsIndexMap.get(column);
        if (cosmosSqlColumn == null) {
            log.error("Column not found: {}", this.localItems);
        }
        return cosmosSqlColumn.getName();
    }

    public int getColumnType(int column) {
//        log.info("getColumnType {}", column);
        CosmosSqlColumn cosmosSqlColumn = columnsIndexMap.get(column);
        if (cosmosSqlColumn == null) {
            log.error("Column not found: {}", this.localItems);
        }
        return cosmosSqlColumn.getType();
    }

    private int getType(Object obj) {
//        log.info("getType {}", obj != null ? obj.getClass() : null);
        if (obj instanceof Integer) {
//            log.info("getType {}", Types.INTEGER);
            return Types.INTEGER;
        }

        if (obj == null) {
//            log.info("getType {}", Types.NULL);
            return Types.NULL;
        }

        if (obj instanceof Long) {
//            log.info("getType {}", Types.BIGINT);
            return Types.BIGINT;
        }

        if (obj instanceof Double) {
//            log.info("getType {}", Types.DOUBLE);
            return Types.DOUBLE;
        }

        if (obj instanceof Float) {
//            log.info("getType {}", Types.FLOAT);
            return Types.FLOAT;
        }

        if (obj instanceof BigDecimal) {
//            log.info("getType {}", Types.DECIMAL);
            return Types.DECIMAL;
        }

        if (obj instanceof Date) {
//            log.info("getType {}", Types.DATE);
            return Types.DATE;
        }

        if (obj instanceof Time) {
//            log.info("getType {}", Types.TIME);
            return Types.TIME;
        }

        if (obj instanceof Timestamp) {
//            log.info("getType {}", Types.TIMESTAMP);
            return Types.TIMESTAMP;
        }

        if (obj instanceof Boolean) {
//            log.info("getType {}", Types.BOOLEAN);
            return Types.BOOLEAN;
        }

        if (obj instanceof byte[]) {
//            log.info("getType {}", Types.BINARY);
            return Types.BINARY;
        }

        if (obj instanceof URL) {
//            log.info("getType {}", Types.DATALINK);
            return Types.DATALINK;
        }

        String string = obj.toString();
        if (string.contains("T") && string.contains(":") && string.contains("-")) {
            try {
                OffsetDateTime.parse(string);
                return Types.TIMESTAMP;
            } catch (Exception ignored) {
            }
        }
        return Types.VARCHAR;
    }

    public SimpleCosmosSqlResultSet(CosmosDbConnection connection) {
        this.connection = connection;
    }

    public SimpleCosmosSqlResultSet(CosmosDbConnection connection, List<LinkedHashMap> objects) {
        this.connection = connection;
        this.localItems = objects;
        this.localItems.stream().forEach(obj -> {
            Set<Map.Entry<String, Object>> set = obj.entrySet();
            set.forEach(entry -> {
                String key = entry.getKey();
                Object value = entry.getValue();
                boolean alreadySaved = columnsMap.containsKey(key);
                if (!alreadySaved) {
                    log.info("Adding column: {}", key);
                    int type = getType(value);
                    CosmosSqlColumn column = new CosmosSqlColumn(key, type);
                    columnsMap.put(key, column);
                    columnsIndexMap.put(columnsIndexMap.size() + 1, column);
                }
            });
        });
    }

    private boolean hasNext() {
        return localItems.size() - 1 >= currentRowIndex;
    }

    private Object getValue(int columnIndex) {
        log.info("SearchValue {} {}", columnIndex, currentRowIndex);
        LinkedHashMap currentRow = localItems.get(currentRowIndex);
        CosmosSqlColumn cosmosSqlColumn = columnsIndexMap.get(columnIndex);
        String columnName = cosmosSqlColumn.getName();
        return currentRow.get(columnName);
    }

    @Override
    public boolean next() throws SQLException {
        if (hasNext()) {
            currentRowIndex++;
//            log.info("found next row: {}", currentRowIndex);
        }
        return hasNext();
    }

    @Override
    public void close() throws SQLException {
        log.info("close");
        localItems = new ArrayList<>();
        currentRowIndex = -1;
        connection = null;
    }

    @Override
    public boolean wasNull() throws SQLException {
        log.info("wasNull");
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        log.info("getString {} {}", columnIndex, currentRowIndex);
        Object value = getValue(columnIndex);
        return value == null ? null : value.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        log.info("getBoolean {} {}", columnIndex, currentRowIndex);
        return false;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        log.info("getByte {} {}", columnIndex, currentRowIndex);
        return 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        log.info("getShort {} {}", columnIndex, currentRowIndex);
        return 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        log.info("getInt {} {}", columnIndex, currentRowIndex);
        return 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        log.info("getLong {} {}", columnIndex, currentRowIndex);
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        log.info("getFloat {} {}", columnIndex, currentRowIndex);
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        log.info("getDouble {} {}", columnIndex, currentRowIndex);
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        log.info("getBigDecimal {} {}", columnIndex, currentRowIndex);
        return null;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        log.info("getBytes {} {}", columnIndex, currentRowIndex);
        return new byte[0];
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        log.info("getDate {} {}", columnIndex, currentRowIndex);
        return null;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        log.info("getTime {} {}", columnIndex, currentRowIndex);
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        log.info("getTimestamp {} {}", columnIndex, currentRowIndex);
        return null;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        log.info("getAsciiStream {} {}", columnIndex, currentRowIndex);
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        log.info("getUnicodeStream {} {}", columnIndex, currentRowIndex);
        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        log.info("getBinaryStream {} {}", columnIndex, currentRowIndex);
        return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        log.info("getString {} {}", columnLabel, currentRowIndex);
        LinkedHashMap currentRow = localItems.get(currentRowIndex);
        return currentRow.get(columnLabel).toString();
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        log.info("getBoolean {} {}", columnLabel, currentRowIndex);
        return false;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        log.info("getByte {} {}", columnLabel, currentRowIndex);
        return 0;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        log.info("getShort {} {}", columnLabel, currentRowIndex);
        return 0;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        log.info("getInt {} {}", columnLabel, currentRowIndex);
        return 0;
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        log.info("getLong {} {}", columnLabel, currentRowIndex);
        return 0;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        log.info("getFloat {} {}", columnLabel, currentRowIndex);
        return 0;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        log.info("getDouble {} {}", columnLabel, currentRowIndex);
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        log.info("getBigDecimal {} {}", columnLabel, currentRowIndex);
        return null;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        log.info("getBytes {} {}", columnLabel, currentRowIndex);
        return new byte[0];
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        log.info("getDate {} {}", columnLabel, currentRowIndex);
        return null;
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        log.info("getTime {} {}", columnLabel, currentRowIndex);
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        log.info("getTimestamp {} {}", columnLabel, currentRowIndex);
        return null;
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        log.info("getAsciiStream {} {}", columnLabel, currentRowIndex);
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        log.info("getUnicodeStream {} {}", columnLabel, currentRowIndex);
        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        log.info("getBinaryStream {} {}", columnLabel, currentRowIndex);
        return null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        log.info("getWarnings");
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        log.info("clearWarnings");
    }

    @Override
    public String getCursorName() throws SQLException {
        log.info("getCursorName");
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        log.info("getResultSetMetaData");
        return new CosmosSqlResultSetMetadata(this);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        log.info("getObject ci {} {}", columnIndex, currentRowIndex);
        return getValue(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        log.info("getObject cl {} {}", columnLabel, currentRowIndex);

        LinkedHashMap currentRow = localItems.get(currentRowIndex);
        return currentRow.get(columnLabel);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        log.info("findColumn {} {}", columnLabel, currentRowIndex);

        int columnIndex = columnsIndexMap.entrySet()
                .stream()
                .filter(entry -> entry.getValue().name.equals(columnLabel))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(-1);

        if (columnIndex == -1) {
            throw new SQLException("Column not found: " + columnLabel);
        }

        return columnIndex;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        log.info("getCharacterStream {} {}", columnIndex, currentRowIndex);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        log.info("getCharacterStream {} {}", columnLabel, currentRowIndex);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        log.info("getBigDecimal {} {}", columnIndex, currentRowIndex);
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        log.info("getBigDecimal {} {}", columnLabel, currentRowIndex);
        return null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        log.info("isBeforeFirst");
        return currentRowIndex == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        log.info("isAfterLast");
        return currentRowIndex > localItems.size() - 1;
    }

    @Override
    public boolean isFirst() throws SQLException {
        log.info("isFirst");
        return currentRowIndex == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        log.info("isLast");
        return currentRowIndex == localItems.size() - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        log.info("beforeFirst");
        currentRowIndex = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        log.info("afterLast");
        //TODO: calculate total and set +1
        currentRowIndex = Integer.MAX_VALUE;
    }

    @Override
    public boolean first() throws SQLException {
        log.info("first");
        currentRowIndex = 0;
        return !localItems.isEmpty();
    }

    @Override
    public boolean last() throws SQLException {
        log.info("last");
        currentRowIndex = localItems.size() - 1;
        return !localItems.isEmpty();
    }

    @Override
    public int getRow() throws SQLException {
        log.info("getRow");
        return currentRowIndex + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        log.info("absolute {}", row);

        int newIndex = row - 1;

        if (newIndex < 0) {
            currentRowIndex = -1;
            return false;
        }

        if (newIndex >= localItems.size()) {
            currentRowIndex = localItems.size();
            return false;
        }

        return currentRowIndex >= 0 && currentRowIndex < localItems.size();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        log.info("relative {}", rows);

        int newIndex = currentRowIndex + rows;

        if (newIndex < 0) {
            currentRowIndex = -1;
            return false;
        }

        if (newIndex >= localItems.size()) {
            currentRowIndex = localItems.size();
            return false;
        }

        return currentRowIndex >= 0 && currentRowIndex < this.localItems.size();
    }

    @Override
    public boolean previous() throws SQLException {
        log.info("previous");

        if (currentRowIndex > 0) {
            currentRowIndex--;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        log.info("setFetchDirection {}", direction);
        //TODO
    }

    @Override
    public int getFetchDirection() throws SQLException {
        log.info("getFetchDirection");
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        log.info("setFetchSize {}", rows);
        fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        log.info("getFetchSize");
        return fetchSize;
    }

    @Override
    public int getType() throws SQLException {
//        log.info("getType");
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        log.info("getConcurrency");
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        log.info("rowUpdated");
        //TODO
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        log.info("rowInserted");
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        log.info("rowDeleted");
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        log.info("updateNull {}", columnIndex);
        //TODO
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        log.info("updateBoolean {} {}", columnIndex, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        log.info("updateByte {} {}", columnIndex, x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        log.info("updateShort {} {}", columnIndex, x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        log.info("updateInt {} {}", columnIndex, x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        log.info("updateLong {} {}", columnIndex, x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        log.info("updateFloat {} {}", columnIndex, x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        log.info("updateDouble {} {}", columnIndex, x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        log.info("updateBigDecimal {} {}", columnIndex, x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        log.info("updateString {} {}", columnIndex, x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        log.info("updateBytes {} {}", columnIndex, x);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        log.info("updateDate {} {}", columnIndex, x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        log.info("updateTime {} {}", columnIndex, x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        log.info("updateTimestamp {} {}", columnIndex, x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        log.info("updateAsciiStream {} {}", columnIndex, x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        log.info("updateBinaryStream {} {}", columnIndex, x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        log.info("updateCharacterStream {} {}", columnIndex, x);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        log.info("updateObject {} {}", columnIndex, x);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        log.info("updateObject {} {}", columnIndex, x);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        log.info("updateNull {}", columnLabel);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        log.info("updateBoolean {} {}", columnLabel, x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        log.info("updateByte {} {}", columnLabel, x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        log.info("updateShort {} {}", columnLabel, x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        log.info("updateInt {} {}", columnLabel, x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        log.info("updateLong {} {}", columnLabel, x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        log.info("updateFloat {} {}", columnLabel, x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        log.info("updateDouble {} {}", columnLabel, x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        log.info("updateBigDecimal {} {}", columnLabel, x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        log.info("updateString {} {}", columnLabel, x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        log.info("updateBytes {} {}", columnLabel, x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        log.info("updateDate {} {}", columnLabel, x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        log.info("updateTime {} {}", columnLabel, x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        log.info("updateTimestamp {} {}", columnLabel, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        log.info("updateAsciiStream {} {}", columnLabel, x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        log.info("updateBinaryStream {} {}", columnLabel, x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        log.info("updateCharacterStream {} {}", columnLabel, reader);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        log.info("updateObject {} {}", columnLabel, x);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        log.info("updateObject {} {}", columnLabel, x);
    }

    @Override
    public void insertRow() throws SQLException {
        log.info("insertRow");
    }

    @Override
    public void updateRow() throws SQLException {
        log.info("updateRow");
    }

    @Override
    public void deleteRow() throws SQLException {
        log.info("deleteRow");
    }

    @Override
    public void refreshRow() throws SQLException {
        log.info("refreshRow");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        log.info("cancelRowUpdates");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        log.info("moveToInsertRow");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        log.info("moveToCurrentRow");
    }

    @Override
    public Statement getStatement() throws SQLException {
        log.info("getStatement");
        return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        log.info("getObject ma {} {}", columnIndex, map);
        //TODO
        return getObject(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        log.info("getRef {}", columnIndex);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        log.info("getBlob {}", columnIndex);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        log.info("getClob {}", columnIndex);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        log.info("getArray {}", columnIndex);

        Object value = getValue(columnIndex);

        if (value instanceof List) {
            return connection.createArrayOf("ARRAY", ((List) value).toArray());
        }

        if (value instanceof Object[]) {
            return connection.createArrayOf("ARRAY", (Object[]) value);
        }

        return connection.createArrayOf("ARRAY", new Object[]{value});
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        log.info("getObject malab {} {}", columnLabel, map);

        return getValue(findColumn(columnLabel));
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        log.info("getRef {}", columnLabel);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        log.info("getBlob {}", columnLabel);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        log.info("getClob {}", columnLabel);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        log.info("getArray {}", columnLabel);
        return getArray(findColumn(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        log.info("getDate {} {}", columnIndex, cal);
        //TODO
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        log.info("getDate {} {}", columnLabel, cal);
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        log.info("getTime {} {}", columnIndex, cal);
        //TODO: calendar not used
        Object value = getValue(columnIndex);
        return convertToTime(value);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        log.info("getTime {} {}", columnLabel, cal);
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        log.info("getTimestamp {} {}", columnIndex, cal);

        Object value = getValue(columnIndex);
        return convertToTimestamp(value);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        log.info("getTimestamp {} {}", columnLabel, cal);
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        log.info("getURL {}", columnIndex);
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        log.info("getURL {}", columnLabel);
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        log.info("updateRef {} {}", columnIndex, x);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        log.info("updateRef {} {}", columnLabel, x);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        log.info("updateBlob {} {}", columnIndex, x);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        log.info("updateBlob {} {}", columnLabel, x);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        log.info("updateClob {} {}", columnIndex, x);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        log.info("updateClob {} {}", columnLabel, x);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        log.info("updateArray {} {}", columnIndex, x);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        log.info("updateArray {} {}", columnLabel, x);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        log.info("getRowId {}", columnIndex);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        log.info("getRowId {}", columnLabel);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        log.info("updateRowId {} {}", columnIndex, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        log.info("updateRowId {} {}", columnLabel, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        log.info("getHoldability");
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        log.info("isClosed");
        return connection.isClosed();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        log.info("updateNString {} {}", columnIndex, nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        log.info("updateNString {} {}", columnLabel, nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        log.info("updateNClob {} {}", columnIndex, nClob);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        log.info("updateNClob {} {}", columnLabel, nClob);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        log.info("getNClob {}", columnIndex);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        log.info("getNClob {}", columnLabel);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        log.info("getSQLXML {}", columnIndex);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        log.info("getSQLXML {}", columnLabel);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        log.info("updateSQLXML {} {}", columnIndex, xmlObject);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        log.info("updateSQLXML {} {}", columnLabel, xmlObject);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        log.info("getNString {}", columnIndex);
        return "";
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        log.info("getNString {}", columnLabel);
        return "";
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        log.info("getNCharacterStream {}", columnIndex);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        log.info("getNCharacterStream {}", columnLabel);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        log.info("updateNCharacterStream {} {}", columnIndex, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        log.info("updateNCharacterStream {} {}", columnLabel, reader);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        log.info("updateAsciiStream {} {}", columnIndex, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        log.info("updateBinaryStream {} {}", columnIndex, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        log.info("updateCharacterStream {} {}", columnIndex, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        log.info("updateAsciiStream {} {}", columnLabel, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        log.info("updateBinaryStream {} {}", columnLabel, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        log.info("updateCharacterStream {} {}", columnLabel, reader);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        log.info("updateBlob {} {}", columnIndex, inputStream);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        log.info("updateBlob {} {}", columnLabel, inputStream);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        log.info("updateClob {} {}", columnIndex, reader);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        log.info("updateClob {} {}", columnLabel, reader);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        log.info("updateNClob {} {}", columnIndex, reader);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        log.info("updateNClob {} {}", columnLabel, reader);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        log.info("updateNCharacterStream {} {}", columnIndex, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        log.info("updateNCharacterStream {} {}", columnLabel, reader);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        log.info("updateAsciiStream {} {}", columnIndex, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        log.info("updateBinaryStream {} {}", columnIndex, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        log.info("updateCharacterStream {} {}", columnIndex, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        log.info("updateAsciiStream {} {}", columnLabel, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        log.info("updateBinaryStream {} {}", columnLabel, x);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        log.info("updateCharacterStream {} {}", columnLabel, reader);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        log.info("updateBlob {} {}", columnIndex, inputStream);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        log.info("updateBlob {} {}", columnLabel, inputStream);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        log.info("updateClob {} {}", columnIndex, reader);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        log.info("updateClob {} {}", columnLabel, reader);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        log.info("updateNClob {} {}", columnIndex, reader);

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        log.info("updateNClob {} {}", columnLabel, reader);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        log.info("getObject types t {} {}", columnIndex, type);

        Object object = getObject(columnIndex);

        if (object == null) {
            return null;
        }

        if (type.isInstance(object)) {
            return (T) object;
        }

        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        log.info("getObject tpy tpe {} {}", columnLabel, type);
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        log.info("unwrap {}", iface);
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        log.info("isWrapperFor {}", iface);
        return false;
    }

    private Time convertToTime(Object right) {
        if (right instanceof Time) {
            return (Time) right;
        }

        if (right instanceof Long) {
            return new Time((Long) right);
        }

        if (right instanceof String) {
            return Time.valueOf((String) right);
        }

        if (right instanceof Calendar) {
            return new Time(((Calendar) right).getTimeInMillis());
        }

        return null;
    }

    private Timestamp convertToTimestamp(Object right) {
        if (right instanceof Timestamp) {
            return (Timestamp) right;
        }

        if (right instanceof Long) {
            return new Timestamp((Long) right);
        }

        if (right instanceof String) {
            return Timestamp.valueOf((String) right);
        }

        if (right instanceof Calendar) {
            return new Timestamp(((Calendar) right).getTimeInMillis());
        }

        return null;
    }

    @Override
    public String toString() {
        return "CosmosDbResultSet{" +
                "rows=" + localItems +
                ", columns=" + columnsIndexMap.values() +
                '}';
    }
}
