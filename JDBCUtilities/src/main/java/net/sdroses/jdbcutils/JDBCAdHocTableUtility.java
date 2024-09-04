/**
 * Created on 8/20/2024
 */
package net.sdroses.jdbcutils;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;

import lombok.Getter;
import lombok.Setter;
import net.sdroses.jdbcutils.order.OrderBy;
import net.sdroses.jdbcutils.order.OrderByClause;
import net.sdroses.jdbcutils.wrapper.QueryWrapper;

/**
 * Utility class for generating and/or executing simple selects, inserts, updates, and deletes on a table using JdbcTemplate
 * 
 * @author Steve Rose
 */
public class JDBCAdHocTableUtility {

	/**
	 * <p>Pairs a value with JDBCColumnMetaData, used internally when inserting and updating
	 * <p>{@link JDBCColumnMetaData} columnMetaData
	 * <p>String value
	 */
	private record JDBCColumnMetaDataWithValue(JDBCColumnMetaData columnMetaData, String value) {}

	/**
	 * <p>Pairs a parameterized SQL Statement along with a list of columns and corresponding value ({@link JDBCColumnMetaDataWithValue})
	 */
	private record SQLStatementWithValues(String sql, ArrayList<JDBCColumnMetaDataWithValue> parameterData) {}

	/**
	 * Default date format
	 */
	public static String defaultStandardDateFormat = "MM/dd/yyyy";

	/**
	 * Default time format
	 */
	public static String defaultStandardTimeFormat = "HH:mm:ss";
	
	/**
	 * {@link JDBCTableMetaData}
	 */
	@Getter @Setter private JDBCTableMetaData tableMetaData;
	
	/**
	 * {@link JdbcTemplate}
	 */
	@Getter @Setter private JdbcTemplate jdbcTemplate;

	/**
	 * Date format to use when converting a date/datetime/timestamp to and from a String
	 */
	@Getter @Setter private String dateFormat = defaultStandardDateFormat;
	
	/**
	 * Time format to use when converting a time/datetime/timestamp to and from a String
	 */
	@Getter @Setter private String timeFormat = defaultStandardTimeFormat;

	/**
	 * Default constructor, uses default date and time formats.
	 * @param tableMetaData
	 * @param jdbcTemplate
	 */
	public JDBCAdHocTableUtility(JDBCTableMetaData tableMetaData, JdbcTemplate jdbcTemplate) {
		this.tableMetaData = tableMetaData;
		this.jdbcTemplate = jdbcTemplate;
	}

	/**
	 * Full constructor
	 * @param tableMetaData
	 * @param jdbcTemplate
	 * @param dateFormat
	 * @param timeFormat
	 */
	public JDBCAdHocTableUtility(JDBCTableMetaData tableMetaData, JdbcTemplate jdbcTemplate, String dateFormat, String timeFormat) {
		this(tableMetaData, jdbcTemplate);
		this.dateFormat = dateFormat;
		this.timeFormat = timeFormat;
	}

	/**
	 * <p>Perform a select operation on the table referenced in {@link #tableMetaData} using the supplied
	 * keyField/value pairs in a Properties object.
	 * <p><b>Note:</b> The table definition must have defined keys, otherwise all records will be selected!
	 * @param keyData {@link Properties} list of keys with values. The key field names must match the database exactly, including case.
	 * @param orderBy {@link OrderByClause} optional collection of {@link OrderBy} entries 
	 * @param queryWrapper {@link QueryWrapper} optional utility to wrap the generated query inside an outer query for implementation specific purposes
	 */
	public ArrayList<Properties> select(Properties keyData, OrderByClause orderBy, QueryWrapper queryWrapper) {
		
		SQLStatementWithValues sqlStatementWithValues = prepareSelectStatement(keyData, orderBy, queryWrapper);
		
		ArrayList<Properties> resultData = new ArrayList<Properties>();

		jdbcTemplate.query(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				PreparedStatement pstmt = con.prepareStatement(sqlStatementWithValues.sql);				
				setQueryParameters(sqlStatementWithValues.parameterData, pstmt);
				return pstmt;
			}
		}, rs -> {
			resultData.add(resultSetToProperties(rs));
		});
		
		return resultData;
	}

	/**
	 * <p>Perform an insert operation on the table referenced in tableMetaData using the supplied
	 * field/value pairs in a Properties object.
	 * @param data {@link Properties} list of fields with values. The field names must match the database exactly, including case.
	 * @return a Properties list containing generated keys. This can be null if no keys are defined for this table.
	 */
	public Properties insert(Properties data) {
				
		SQLStatementWithValues sqlStatementWithValues = prepareInsertStatement( data);
		
		// get a list of key fields that will be generated and returned when the insert statement is executed 
		String [] keys = tableMetaData.keyNames.toArray(String[]::new);
		Properties generatedKeys = new Properties();

		jdbcTemplate.execute(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				return con.prepareStatement(sqlStatementWithValues.sql, keys);
			}
		}, new PreparedStatementCallback<Boolean>(){  
		    @Override  
		    public Boolean doInPreparedStatement(PreparedStatement pstmt)  
		            throws SQLException, DataAccessException {  
		              
		    	setQueryParameters(sqlStatementWithValues.parameterData, pstmt);
								
				boolean result = pstmt.execute();
				
				ResultSet rs = pstmt.getGeneratedKeys();
				if (rs.next()) {
					for (String key : keys) {
						generatedKeys.setProperty(key, rs.getString(key));
					}
				}

				return result;
		    }
		});
		
		return generatedKeys;
	}

	/**
	 * <p>Perform an update operation on the table referenced in tableMetaData using the supplied
	 * field/value pairs in a Properties object. Keys are pulled from the data based on the table definition.
	 * <p><b>Note:</b> keys cannot be updated with this call.
	 * <p><b>Note:</b> The table definition must have defined keys, otherwise all records could be updated!
	 * @param data {@link Properties} list of fields with values. The field names must match the database exactly, including case.
	 */
	public boolean update(Properties data) {
		SQLStatementWithValues sqlStatementWithValues = prepareUpdate(data);

		return executeUpdate(sqlStatementWithValues.sql, sqlStatementWithValues.parameterData);
	}

	/**
	 * <p>Perform an update operation on the table referenced in tableMetaData using the supplied
	 * field/value pairs in a Properties object. Keys are specified separately from the data. Any key values in the data will be updated.
	 * <p><b>Note:</b> keys <i>can</i> be updated with this call.
	 * @param data {@link Properties} list of fields with values. The field names must match the database exactly, including case.
	 * @param keyData {@link Properties} list of keys with values. The key field names must match the database exactly, including case.
	 */
	public boolean update(Properties data, Properties keyData) {
		SQLStatementWithValues sqlStatementWithValues = prepareUpdate(data, keyData);
				
		return executeUpdate(sqlStatementWithValues.sql, sqlStatementWithValues.parameterData);
	}

	/**
	 * <p>Perform a delete operation on the table referenced in tableMetaData using the supplied
	 * keyField/value pairs in a Properties object.
	 * <p><b>Note:</b> The table definition must have defined keys, otherwise all records will be deleted!
	 * @param keyData {@link Properties} list of keys with values. The key field names must match the database exactly, including case.
	 */
	public boolean delete(Properties keyData) {
		SQLStatementWithValues sqlStatementWithValues = prepareDelete(keyData);
				
		return executeUpdate(sqlStatementWithValues.sql, sqlStatementWithValues.parameterData);
	}

	/**
	 * <p>Builds a parameterized SQL select statement as a string and populates a List containing a reference to the key columns 
	 * and associated keys to use when executing the query. 
	 * @param keyData {@link Properties} containing the key column names and values as Strings
	 * @param orderBy {@link OrderByClause} optional collection of {@link OrderBy} entries 
	 * @param queryWrapper {@link QueryWrapper} optional utility to wrap the generated query inside an outer query for implementation specific purposes
	 * @return {@link SQLStatementWithValues}
	 */
	public SQLStatementWithValues prepareSelectStatement(Properties keyData, OrderByClause orderBy, QueryWrapper queryWrapper) {
		// This List structure is used to match up the column metadata and value for each parameter. It is populated by the built step, 
		// and then used to prepare the statement.
		ArrayList<JDBCColumnMetaDataWithValue> parameterData = new ArrayList<JDBCColumnMetaDataWithValue>();
		
		Set<String> keys = (keyData != null) ? keyData.stringPropertyNames() : null;
		
		String sqlTemplate = buildSelectStatement(keys, orderBy, queryWrapper, columnName -> {
			parameterData.add(new JDBCColumnMetaDataWithValue(tableMetaData.getColumnsbyName().get(columnName), keyData.getProperty((String)columnName)));
		});
		
		SQLStatementWithValues sqlStatementWithValues = new SQLStatementWithValues(sqlTemplate, parameterData);
		return sqlStatementWithValues;
	}

	/**
	 * <p>Builds a parameterized SQL insert statement as a string and populates a List containing a reference to the columns 
	 * and associated values to insert.
	 * @param data {@link Properties} containing the column names and values as Strings
	 * @return {@link SQLStatementWithValues}
	 */
	public SQLStatementWithValues prepareInsertStatement(Properties data) {
		// This List structure is used to match up the column metadata and value for each parameter. It is populated by the built step, 
		// and then used to prepare the statement.
		ArrayList<JDBCColumnMetaDataWithValue> parameterData = new ArrayList<JDBCColumnMetaDataWithValue>(data.size());
		
		String sqlTemplate = buildInsertStatement(data.stringPropertyNames(), columnName -> {
			parameterData.add(new JDBCColumnMetaDataWithValue(tableMetaData.getColumnsbyName().get(columnName), data.getProperty((String)columnName)));
		});
		
		SQLStatementWithValues sqlStatementWithValues = new SQLStatementWithValues(sqlTemplate, parameterData);
		return sqlStatementWithValues;
	}

	/**
	 * <p>Builds a parameterized SQL update statement as a string and populates a List containing a reference to the columns 
	 * and associated values. Keys are included in the columns.
	 * @param data {@link Properties} containing the column names and values as Strings
	 * @return {@link SQLStatementWithValues}
	 */
	public SQLStatementWithValues prepareUpdate(Properties data) {
		// This List structure is used to match up the column metadata and value for each parameter. It is populated by the built step, 
		// and then used to prepare the statement.
		ArrayList<JDBCColumnMetaDataWithValue> parameterData = new ArrayList<JDBCColumnMetaDataWithValue>(data.size());
		
		String sqlTemplate = buildUpdateStatement(data.stringPropertyNames(), columnName -> {
			parameterData.add(new JDBCColumnMetaDataWithValue(tableMetaData.getColumnsbyName().get(columnName), data.getProperty((String)columnName)));
		});

		SQLStatementWithValues sqlStatementWithValues = new SQLStatementWithValues(sqlTemplate, parameterData);
		return sqlStatementWithValues;
	}

	/**
	 * <p>Builds a parameterized SQL update statement as a string and populates a List containing a reference to the columns, keys, 
	 * and associated values. Keys columns may be included in the columns, but will be updated along with the non-key columns. Actual
	 * keys used in the where clause come from the keyData Properties.
	 * @param data {@link Properties} containing the column names and values as Strings
	 * @param keyData {@link Properties} containing the key column names and values as Strings
	 * @return {@link SQLStatementWithValues}
	 */
	public SQLStatementWithValues prepareUpdate(Properties data, Properties keyData) {
		// This List structure is used to match up the column metadata and value for each parameter. It is populated by the built step, 
		// and then used to prepare the statement.
		ArrayList<JDBCColumnMetaDataWithValue> parameterData = new ArrayList<JDBCColumnMetaDataWithValue>(data.size());
		
		// This one is a little different. Since this version allows updating of key values, the prepared statement needs to contain
		// both values. Since we don't pass in the values for fields being updated, the action lambda will return a value only for keys
		String sqlTemplate = buildUpdateStatement(data.stringPropertyNames(), keyData, (columnName, value) -> {
			parameterData.add(new JDBCColumnMetaDataWithValue(tableMetaData.getColumnsbyName().get(columnName), value != null ? value : data.getProperty((String)columnName)));
		});

		SQLStatementWithValues sqlStatementWithValues = new SQLStatementWithValues(sqlTemplate, parameterData);
		return sqlStatementWithValues;
	}

	/**
	 * <p>Builds a parameterized SQL delete statement as a string and populates a List containing a reference to the key columns 
	 * and associated values to use when executing the delete. 
	 * @param keyData {@link Properties} containing the key column names and values as Strings
	 * @return {@link SQLStatementWithValues}
	 */
	public SQLStatementWithValues prepareDelete(Properties keyData) {
		// This List structure is used to match up the column metadata and value for each parameter. It is populated by the built step, 
		// and then used to prepare the statement.
		ArrayList<JDBCColumnMetaDataWithValue> parameterData = new ArrayList<JDBCColumnMetaDataWithValue>(keyData.size());
		
		String sqlTemplate = buildDeleteStatement(keyData.stringPropertyNames(), columnName -> {
			parameterData.add(new JDBCColumnMetaDataWithValue(tableMetaData.getColumnsbyName().get(columnName), keyData.getProperty((String)columnName)));
		});

		SQLStatementWithValues sqlStatementWithValues = new SQLStatementWithValues(sqlTemplate, parameterData);
		return sqlStatementWithValues;
	}

	/**
	 * <p>Wrapper function to handle update and delete statements. 
	 * @param sqlTemplate
	 * @param parameterData
	 */
	private boolean executeUpdate(String sqlTemplate, ArrayList<JDBCColumnMetaDataWithValue> parameterData) {
		
		return jdbcTemplate.execute(sqlTemplate, new PreparedStatementCallback<Boolean>(){  
		    @Override  
		    public Boolean doInPreparedStatement(PreparedStatement pstmt)  
		            throws SQLException, DataAccessException {  
		              
		    	setQueryParameters(parameterData, pstmt);
								
				return pstmt.execute();
		    }
		});
	}

	/**
	 * <p>Build a select statement as a String using the table definition from {@link #tableMetaData} and a Collection of key column names
	 * for the where clause. 
	 * @param keyData Collection of column names
	 * @param orderBy {@link OrderByClause} optional collection of {@link OrderBy} entries 
	 * @param queryWrapper {@link QueryWrapper} optional utility to wrap the generated query inside an outer query for implementation specific purposes
	 * @param action optional lambda that is executed once for each key column.
	 * @return
	 */
	public String buildSelectStatement(Collection<String> keyData, OrderByClause orderBy, QueryWrapper queryWrapper, Consumer<String> action) {
		StringBuilder sb = new StringBuilder();
		sb.append("select * from " + tableMetaData.getFullTableName());

		boolean first = true;
		if (keyData != null && keyData.size() > 0) {
			for (String keyColumnName : keyData) {
				if (!tableMetaData.columnsbyName.containsKey(keyColumnName)) {
					continue;
				}
				if (action != null) {
					action.accept(keyColumnName);
				}
				if (first) {
					sb.append(" where ");
					first = false;
				} else {
					sb.append(" and ");
				}
				sb.append(keyColumnName + " = ?");
			}
		}
		if (orderBy != null) {
			sb.append(" ");
			sb.append(orderBy);
		}

		if (queryWrapper != null) {
			return queryWrapper.wrap(sb.toString(), "z");
		}
		return sb.toString();
	}

	/**
	 * <p>Build an insert statement as a String using the table definition from {@link #tableMetaData} and a Collection of column names
	 * 
	 * @param columnNames Collection of column names
	 * @param action optional lambda that is executed once for each column.
	 * @return
	 */
	public String buildInsertStatement(Collection<String> columnNames, Consumer<String> action) {
		StringBuilder sb = new StringBuilder();
		StringBuilder sbParams = new StringBuilder();

		sb.append("insert into " + tableMetaData.getFullTableName() + " (");
		sbParams.append(") values (");

		boolean first = true;
		for (String columnName : columnNames) {
			if (!tableMetaData.columnsbyName.containsKey(columnName)) {
				continue;
			}
			if (action != null) {
				action.accept(columnName);
			}
			if (first) {
				first = false;
			} else {
				sb.append(", ");
				sbParams.append(", ");
			}
			sb.append(columnName);
			sbParams.append("?");
		}
		
		sb.append(sbParams + ");");
		String insertTemplate = sb.toString();
		return insertTemplate;
	}

	/**
	 * <p>Build an update statement as a String using the table definition from {@link #tableMetaData} and a Collection of column names.
	 * Key columns are placed in the where clause. 
	 * @param columnNames Collection of column names
	 * @param action optional lambda that is executed once for each key column.
	 * @return
	 */
	public String buildUpdateStatement(Collection<String> columnNames, Consumer<String> action) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("update " + tableMetaData.getFullTableName() + " set ");
		boolean first = true;
		Set<String> keys = tableMetaData.getKeyNames();
		for (String columnName : columnNames) {
			if (!tableMetaData.columnsbyName.containsKey(columnName)) {
				continue;
			}
			if (action != null) {
				action.accept(columnName);
			}
			if (first) {
				first = false;
			} else {
				sb.append(", ");
			}
			sb.append(columnName + " = ?");
		}
		first = true;
		if (keys != null && keys.size() > 0) {
			for (String keyColumnName : keys) {
				if (!tableMetaData.columnsbyName.containsKey(keyColumnName)) {
					continue;
				}
				if (action != null) {
					action.accept((String)keyColumnName);
				}
				if (first) {
					sb.append(" where ");
					first = false;
				} else {
					sb.append(" and ");
				}
				sb.append(keyColumnName + " = ?");
			}
		}
		sb.append(";");
		return sb.toString();	
	}
	
	/**
	 * <p>Build an update statement as a String using the table definition from {@link #tableMetaData} and a Collection of column names.
	 * Key columns from keyData Properties are placed in the where clause. 
	 * @param columnNames Collection of column names
	 * @param action optional lambda that is executed once for each key column.
	 * @return
	 */
	public String buildUpdateStatement(Collection<String> columnNames, Properties keyData, BiConsumer<String, String> action) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("update " + tableMetaData.getFullTableName() + " set ");
		boolean first = true;
		for (String columnName : columnNames) {
			if (!tableMetaData.columnsbyName.containsKey(columnName)) {
				continue;
			}
			if (action != null) {
				action.accept(columnName, null);
			}
			if (first) {
				first = false;
			} else {
				sb.append(", ");
			}
			sb.append(columnName + " = ?");
		}
		first = true;
		if (keyData != null && keyData.size() > 0) {
			for (Object keyColumnName : keyData.keySet()) {
				if (!tableMetaData.columnsbyName.containsKey(keyColumnName)) {
					continue;
				}
				if (action != null) {
					action.accept((String)keyColumnName, keyData.getProperty((String)keyColumnName));
				}
				if (first) {
					sb.append(" where ");
					first = false;
				} else {
					sb.append(" and ");
				}
				sb.append(keyColumnName + " = ?");
			}
		}
		sb.append(";");
		return sb.toString();	
	}

	/**
	 * <p>Build a delete statement as a String using the table definition from {@link #tableMetaData} and a Collection of key column names
	 * for the where clause. 
	 * @param keyData Collection of column names
	 * @param action optional lambda that is executed once for each key column.
	 * @return
	 */
	public String buildDeleteStatement(Collection<String> keyData, Consumer<String> action) {
		StringBuilder sb = new StringBuilder();
		sb.append("delete from " + tableMetaData.getFullTableName());

		boolean first = true;
		if (keyData != null && keyData.size() > 0) {
			for (String keyColumnName : keyData) {
				if (!tableMetaData.columnsbyName.containsKey(keyColumnName)) {
					continue;
				}
				if (action != null) {
					action.accept(keyColumnName);
				}
				if (first) {
					sb.append(" where ");
					first = false;
				} else {
					sb.append(" and ");
				}
				sb.append(keyColumnName + " = ?");
			}
		}
		sb.append(";");

		return sb.toString();
	}

	/**
	 * Utility function to extract a row from a ResultSet into a Properties object. 
	 * @param rs {@link ResultSet}
	 * @return Properties containing the row values
	 * @throws SQLException
	 */
	public Properties resultSetToProperties(ResultSet rs) throws SQLException {
		Properties row = new Properties();
		ResultSetMetaData rsmd = rs.getMetaData();
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			
			String value = rs.getString(i);
			switch (rsmd.getColumnType(i)) {
			case java.sql.Types.DATE:
			case java.sql.Types.TIMESTAMP:
			case java.sql.Types.TIME:
				value = formatDate(rs.getTimestamp(i), rsmd.getColumnType(i));
				break;
			case java.sql.Types.DECIMAL:
			case java.sql.Types.FLOAT: 
			case java.sql.Types.DOUBLE: 
			case java.sql.Types.NUMERIC: 
			case java.sql.Types.INTEGER:
			case java.sql.Types.BIGINT:
			case java.sql.Types.REAL:
			case java.sql.Types.SMALLINT: 
			case java.sql.Types.TINYINT:
			case java.sql.Types.CLOB:
			default:
				value = rs.getString(i);
			}
			row.setProperty(rsmd.getColumnName(i), (value == null) ? "" : value);
		}
		return row;
	}

	/**
	 * Sets the parameters in a PreparedStatement object 
	 * @param parameterData List of {@link JDBCColumnMetaDataWithValue}
	 * @param pstmt {@link PreparedStatement}
	 * @throws SQLException
	 */
	private void setQueryParameters(ArrayList<JDBCColumnMetaDataWithValue> parameterData, PreparedStatement pstmt) throws SQLException {
		int i = 0;
		// For each field, set the corresponding parameter value based on the data type, converting from String needed.
		for (JDBCColumnMetaDataWithValue paramter : parameterData) {
			JDBCColumnMetaData columnMetaData = paramter.columnMetaData();
			String value = paramter.value();
			if (columnMetaData == null) {
				continue; // Maybe this should be break?
			}
			i++;
			int dataType = columnMetaData.getDataType();			
			if (value == null || value.length() == 0) {
				pstmt.setNull(i, dataType);
				continue;
			}
			switch (dataType) {
			case java.sql.Types.DATE:
			case java.sql.Types.TIME:
			case java.sql.Types.TIMESTAMP:				
	        	prepareDateTime(pstmt, i, value, dataType);
				break;
			case java.sql.Types.DECIMAL:
			case java.sql.Types.FLOAT: 
			case java.sql.Types.DOUBLE: 
			case java.sql.Types.NUMERIC: 
			case java.sql.Types.INTEGER:
			case java.sql.Types.BIGINT:
			case java.sql.Types.REAL:
			case java.sql.Types.SMALLINT: 
			case java.sql.Types.TINYINT:
	        	pstmt.setObject(i, value.replaceAll("[^0-9.-]",""), dataType);
				break;
			case java.sql.Types.CLOB:
	        	pstmt.setCharacterStream(i, new StringReader(value), value.length());
	        	break;
			default:
	        	pstmt.setObject(i, value, dataType);
			}
		}
	}  

	/**
	 * Formats a date and/or time field as a String using {@link #getDateAndOrTimeFormat}
	 * @param date Java Date object
	 * @param dataType int data type from java.sql.Types
	 * @return String representation of date
	 */
	private String formatDate(Date date, int dataType) {
		SimpleDateFormat dateFormater = new SimpleDateFormat(getDateAndOrTimeFormat(dataType));
		dateFormater.setLenient(true);
		return dateFormater.format(date);
	}
	/**
	 * Utility function to set time-based fields
	 * 
	 * @param pstmt {@link java.sql.PreparedStatement}
	 * @param index int parameter index 
	 * @param value String value to be converted
	 * @param dataType int data type from java.sql.Types used to select the correct conversion and field type
	 * 
	 * @throws SQLException
	 */
	private void prepareDateTime(PreparedStatement pstmt, int index, String value, int dataType)
			throws SQLException {
		java.util.Date date = null;
		try {
			// Get the correct format
			String format = getDateAndOrTimeFormat(dataType);
			
			// Convert the string to a date
			int length = Math.min(value.length(), format.length());
			date = new SimpleDateFormat(format.substring(0, length)).parse(value.substring(0, length));

			// Set the parameter based on the type
			if (date == null) {
				pstmt.setNull(index, dataType);
			} else if (dataType == java.sql.Types.DATE) {
				pstmt.setDate(index, new java.sql.Date(date.getTime()));
			} else if (dataType == java.sql.Types.TIME) {
				pstmt.setTime(index, new java.sql.Time(date.getTime()));
			} else {
				pstmt.setTimestamp(index, new java.sql.Timestamp(date.getTime()));
			}
		} catch (ParseException e) {
			pstmt.setNull(index, dataType);
		}
	}

	/**
	 * Gets the correct format string for date and or time field based on {@link #dateFormat} and/or {@link #timeFormat}
	 * @param dataType from {@link java.sql.Types}
	 * @return format string to use when converting this field type
	 */
	public String getDateAndOrTimeFormat(int dataType) {
		String format = null;
		if (dataType == java.sql.Types.DATE) {
			format = dateFormat;
		} else if (dataType == java.sql.Types.TIME) {
			format = timeFormat;
		} else {
			format = dateFormat + " " + timeFormat;
		}
		return format;
	}
}