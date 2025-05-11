/**
 * Created on 9/4/2024
 */
package net.sdroses.jdbcutils;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 
 * @author Steve Rose
 */
@RestController
@RequestMapping("/adhoc")
public class Controller {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private DataSource datasource;

	/**
	 * @param datasource
	 */
	public Controller(DataSource datasource) {
		this.datasource = datasource;
	}

	@GetMapping("/describe/{tableName}")
	private String getTableDescription(@PathVariable String tableName) {

		String catalog = null;
		String schema = null;
		String table = null;
		
		logger.info(tableName);

		String [] databaseSchemaTable = tableName.split("\\.");

		for (String s : databaseSchemaTable) {
			logger.info(s);
		}
		
		switch (databaseSchemaTable.length) {
		case 1:
			table = databaseSchemaTable[0];
			break;
		case 2:
			schema = databaseSchemaTable[0];
			table = databaseSchemaTable[1];
			break;
		default:
			catalog = databaseSchemaTable[0];
			schema = databaseSchemaTable[1];
			table = databaseSchemaTable[2];
		}
		
		Connection connection = null;
		JDBCTableMetaData tableMetaData = null;
		try {
			connection = datasource.getConnection();
			tableMetaData = new JDBCTableMetaData(connection, 
					"".equals(catalog) ? null : catalog, 
					"".equals(schema) ? null : schema, 
					table);			
		} catch (Exception e) {
			e.printStackTrace();
			return "error loading table " + tableName;
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	
		return tableMetaData.toString();		
	}
}
