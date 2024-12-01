/**
 * Created on 8/15/2024
 */
package net.sdroses.jdbcutils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import lombok.Getter;

/**
 * <p>MetaData about a column in a JDBC database as returned from the 
 * {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()}<p>
 * 
 * <p>Created on 8/15/2024
 * 
 * @author Steve Rose
 */
public class JDBCColumnMetaData {

	/*
	 * The following are all the possible column positions when reading metadata about a column from JDBC.
	 * Not all are used in this class. If the missing ones are needed later, they can be added.
	 */
	public static final int TABLE_CAT = 1;
	public static final int TABLE_SCHEM = 2;
	public static final int TABLE_NAME = 3;
	public static final int COLUMN_NAME = 4;
	public static final int DATA_TYPE = 5;
	public static final int TYPE_NAME = 6;
	public static final int COLUMN_SIZE = 7;
	public static final int BUFFER_LENGTH = 8;
	public static final int DECIMAL_DIGITS = 9;
	public static final int NUM_PREC_RADIX = 10;
	public static final int NULLABLE = 11;
	public static final int REMARKS = 12;
	public static final int COLUMN_DEF = 13;
	public static final int SQL_DATA_TYPE = 14;
	public static final int SQL_DATETIME_SUB = 15;
	public static final int CHAR_OCTET_LENGTH = 16;
	public static final int ORDINAL_POSITION = 17;
	public static final int IS_NULLABLE = 18;
	public static final int SCOPE_CATALOG = 19;
	public static final int SCOPE_SCHEMA = 20;
	public static final int SCOPE_TABLE = 21;
	public static final int SOURCE_DATA_TYPE = 22;
	public static final int IS_AUTOINCREMENT = 23;
	public static final int IS_GENERATEDCOLUMN = 24;
	
	
	/**
	 * <p>COLUMN_NAME String => column name
	 * <p>Column 4 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private String columnName;
	
	/**
	 * <p>DATA_TYPE int => SQL type from java.sql.Types
	 * <p>Column 5 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private int dataType;
	
	/**
	 * <p>TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
	 * <p>Column 6 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private String typeName;
	
	/**
	 * <p>COLUMN_SIZE int => column size.
	 * <p>Column 7 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private int columnSize;
	
	/**
	 * <p>DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
	 * <p>Column 9 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private int decimalDigits;
	
	/**
	 * <p>NUM_PREC_RADIX int => Radix (typically either 10 or 2)
	 * <p>Column 10 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private int precisionRadix;
	
	/**
	 * <p>NULLABLE int => is NULL allowed.
	<ul>
	<li>DatabaseMetaData.columnNoNulls - might not allow NULL values
	<li>DatabaseMetaData.columnNullable - definitely allows NULL values
	<li>DatabaseMetaData.columnNullableUnknown - nullability unknown</ul>
	 * <p>Column 11 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private int nullable;
	
	/**
	 * <p>REMARKS String => comment describing column (may be null)
	 * <p>Column 12 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private String remarks;
	
	/**
	 * <p>COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null)
	 * <p>Column 13 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private String defaultValue;
	
	/**
	 * <p>ORDINAL_POSITION int => index of column in table (starting at 1)
	 * <p>Column 17 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private int position;
	
	/**
	 * <p>SCOPE_CATALOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
	 * <p>Column 19 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private String scopeCatalog;
	
	/**
	 * <p>SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
	 * <p>Column 20 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private String scopeSchema;
	
	/**
	 * <p>SCOPE_TABLE String => table name that this the scope of a reference attribute (null if the DATA_TYPE isn't REF)
	 * <p>Column 21 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private String scopeTable;
	
	/**
	 * <p>IS_AUTOINCREMENT String => Indicates whether this column is auto incremented
	<ul>
	<li>YES --- if the column is auto incremented
	<li>NO --- if the column is not auto incremented
	<li>empty string --- if it cannot be determined whether the column is auto incremented</ul>
	 * <p>Column 23 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private String isAutoIncrement;
	
	/**
	 * <p>IS_GENERATEDCOLUMN String => Indicates whether this is a generated column
	<ul>
	<li>YES --- if this a generated column
	<li>NO --- if this not a generated column
	<li>empty string --- if it cannot be determined whether this is a generated column
	</ul>
	 * <p>Column 24 in {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 */
	@Getter private String isGeneratedColumn;
    
	/**
	 * Initializes JDBCColumnMetaData from {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()} ResultSet
	 * 
	 * @param ResultSet from {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String) DatabaseMetaData.getColumns()}
	 * @throws SQLException
	 */
    public JDBCColumnMetaData(ResultSet rs) throws SQLException {
    	columnName = rs.getString(COLUMN_NAME);
    	dataType = rs.getInt(DATA_TYPE);
    	typeName = rs.getString(TYPE_NAME);
    	columnSize = rs.getInt(COLUMN_SIZE);
    	decimalDigits = rs.getInt(DECIMAL_DIGITS);
    	precisionRadix = rs.getInt(NUM_PREC_RADIX);
    	nullable = rs.getInt(NULLABLE);
    	remarks = rs.getString(REMARKS);
    	defaultValue = rs.getString(COLUMN_DEF);
    	position = rs.getInt(ORDINAL_POSITION);
    	scopeCatalog = rs.getString(SCOPE_CATALOG);
    	scopeSchema = rs.getString(SCOPE_SCHEMA);
    	scopeTable = rs.getString(SCOPE_TABLE);
    	isAutoIncrement = rs.getString(IS_AUTOINCREMENT);
    	isGeneratedColumn = rs.getString(IS_GENERATEDCOLUMN);
    }

    public JDBCColumnMetaData(ResultSetMetaData rsmd, int column) {
    	try {
			columnName = rsmd.getColumnName(column);
			dataType = rsmd.getColumnType(column);
			typeName = rsmd.getColumnTypeName(column);
			columnSize = rsmd.getPrecision(column);
			decimalDigits = rsmd.getScale(column);
			
			nullable = rsmd.isNullable(column);
			
			position = column;
			scopeCatalog = rsmd.getCatalogName(column);
			scopeSchema = rsmd.getSchemaName(column);
			scopeTable = rsmd.getTableName(column);
			isAutoIncrement = rsmd.isAutoIncrement(column) ? "YES" : "NO";
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }
    /**
     * Return the name, type and column size as a String
     */
    public String toString() {
        return columnName + " " + typeName + " " + columnSize;
    }

}
