/**
 * Created on 8/15/2024
 */
package net.sdroses.jdbcutils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;

/**
 * JDBCTableMetaData loads metadata including columns and keys from a database table for later use.
 * 
 * <p>Created on 8/15/2024
 * 
 * @author Steve Rose
 */
public class JDBCTableMetaData {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	/**
	 * A catalog name; must match the catalog name as it is stored in the database
	 */
	@Getter protected String catalog;
	
	/**
	 * A schema name; must match the schema name as it is stored in the database
	 */
	@Getter protected String schema;
	
	/**
	 * A table name
	 */
	@Getter protected String tableName;
	
	/**
	 * Names of the primary keys in the designated {@link tableName table} 
	 */
	@Getter protected Set<String> keyNames;
	
	/**
	 * List of columns in the order retrieved from the database
	 */
	@Getter protected ArrayList<JDBCColumnMetaData> columnsInOrder;
	
	/**
	 * Collection of columns by column name
	 */
	@Getter protected Hashtable<String, JDBCColumnMetaData> columnsbyName;
	
	/**
	 * <p>Initializes JDBCTableMetaData from a JDBC {@link java.sql.Connection Connection}, catalog, schema, and table.
	 * <p><strong>MySQL:</strong> use the catalog to specify the desired database. Schema is not used.
	 * <p><strong>Note:</strong>Failure to use the correct catalog/schema filters can result in duplication if multiple catalogs/schemas have the same table by name
	 * 
	 * @param connection a {@link java.sql.Connection}
	 * @param catalog a catalog name; must match the catalog name as it is stored in the database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
	 * @param schema a schema name; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
	 * @param tableName a table name; must match the table name as it is stored in the database
	 */
	public JDBCTableMetaData(Connection con, String catalog, String schema, String tableName) {
		this.catalog = catalog;
		this.schema = schema;
		this.tableName = tableName;
		
		keyNames = new HashSet<String>();
		columnsInOrder = new ArrayList<JDBCColumnMetaData>();
		columnsbyName = new Hashtable<String, JDBCColumnMetaData>();
		
		ResultSet rs = null;
		
        try {
    		// First, try to load the table metadata using the schema and exact table name.
        	DatabaseMetaData dmd = con.getMetaData();
            if (loadTableMetaData(dmd, catalog, schema, tableName)) {
            	return;
            }
            
            // if the above code fails, most likely due to a case difference, loop through all tables looking for a match
    		String [] types = {"TABLE", "VIEW", "ALIAS", "SYNONYM"};
            rs = dmd.getTables(catalog, schema, null, types);
            while (rs.next()) {
                if (tableName.equalsIgnoreCase(rs.getString(3))) {
                	// Since the fields come from the tables result, this should always find the expected columns  
                	if (loadTableMetaData(dmd, rs.getString(JDBCColumnMetaData.TABLE_CAT), rs.getString(JDBCColumnMetaData.TABLE_SCHEM), rs.getString(JDBCColumnMetaData.TABLE_NAME))) return;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
            	try {
					rs.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
            }
        }
	}

	/**
	 * Loads the keys and columns for the specified table 
	 * 
	 * @param dbMetaData {@link java.sql.DatabaseMetaData}
	 * @param catalog a catalog name; must match the catalog name as it is stored in the database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
	 * @param schema a schema name; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
	 * @param tableName a table name; must match the table name as it is stored in the database
	 *
	 * @return boolean indicating that at least one matching column was found
	 *   
	 * @throws SQLException
	 */
	private boolean loadTableMetaData(DatabaseMetaData dbMetaData, String catalog, String schema, String tableName) throws SQLException {
    	boolean gotOne = false;
    	ResultSet rs = null;
    	try {
    		rs = dbMetaData.getPrimaryKeys(catalog, schema, tableName);
        	while (rs.next()) {
        		keyNames.add(rs.getString(JDBCColumnMetaData.COLUMN_NAME));
        	}
        	rs.close();
        	rs = dbMetaData.getColumns(catalog, schema, tableName, null);
            while (rs.next()) {
            	gotOne = true;
            	JDBCColumnMetaData c = new JDBCColumnMetaData(rs);
            	columnsInOrder.add(c);
            	columnsbyName.put(c.getColumnName(), c);
            }
    	} finally {
    		if (rs != null && !rs.isClosed()) {
    			rs.close();
    		}
    	}
        return gotOne;
	}

	/**
	 * @return the fully qualified table name including catalog and schema in the form 
	 * <{@link catalog}>.<{@link schema}>.<{@link tableName}> omitting any null values
	 */
	public String getFullTableName() {
		StringBuilder sb = new StringBuilder();
		if (this.catalog != null) {
			sb.append(this.catalog);
			sb.append(".");
		}
		if (this.schema != null) {
			sb.append(this.schema);
			sb.append(".");
		}
		sb.append("`");
		sb.append(this.tableName);
		sb.append("`");
		
		return sb.toString();
	}
	
	/**
	 * Outputs a partial table description as a String including columns
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.getFullTableName());
		sb.append(" (");
		boolean first = true;
		for (JDBCColumnMetaData columnMetaData : this.getColumnsInOrder()) {
			if (!first) {
				sb.append(", ");
			}
			sb.append(columnMetaData.toString());
		}
		sb.append(")");
		return sb.toString();
	}
}
