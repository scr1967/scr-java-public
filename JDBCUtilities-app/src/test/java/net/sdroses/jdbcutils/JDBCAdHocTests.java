/**
 * Created on Aug 21, 2024
 */
package net.sdroses.jdbcutils;

import net.sdroses.jdbcutils.order.OrderByClause;
import net.sdroses.jdbcutils.order.OrderByDirection;
import net.sdroses.jdbcutils.wrapper.FilterWrapper;
import net.sdroses.jdbcutils.wrapper.MultiQueryWrapper;
import net.sdroses.jdbcutils.wrapper.PaginationWrapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@EnableAutoConfiguration
@SpringBootTest
public class JDBCAdHocTests {

	@Autowired private DataSource datasource;
	@Autowired private JdbcTemplate jdbcTemplate;
	
	@Value("${date-format-string:MM/dd/yyyy}") private String dateFormat;
	
	@Value("${time-format-string:HH:mm:ss}") private String timeFormat;

	private final Logger logger = LoggerFactory.getLogger(getClass());

	Connection connection = null;
	
	@BeforeAll
	public static void setup() {
		// Nothing here yet
	}
	
	private JDBCTableMetaData getTableMetaData(String tableToGet) {
		Connection connection = null;
		JDBCTableMetaData tableMetaData = null;
		try {
			connection = datasource.getConnection();
			assertNotNull(connection);
			tableMetaData = new JDBCTableMetaData(connection, null, null, tableToGet);			
		} catch (Exception e) {
			fail("Exception", e);			
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return tableMetaData;
	}

	@Test
	public void testTableMetaDataFail() {
		JDBCTableMetaData tableMetaData = getTableMetaData("TEST_TABLE_X");
		assertNotNull(tableMetaData);
		assertEquals(tableMetaData.getColumnsInOrder().size(),0);
	}

	@Test
	public void testTableMetaData() {
		String tableToGet = "TEST_TABLE";
		JDBCTableMetaData tableMetaData = getTableMetaData(tableToGet);
		assertNotNull(tableMetaData);
		ArrayList<JDBCColumnMetaData> columnsInOrder = tableMetaData.getColumnsInOrder();
			
		logger.debug(columnsInOrder.toString());
		logger.debug("" + columnsInOrder.size());
		logger.debug(tableMetaData.getFullTableName());
		logger.debug(tableMetaData.getKeyNames().toString());
		logger.debug(tableMetaData.getColumnsbyName().toString());

		assertEquals(columnsInOrder.size(), 11);
		assertTrue(columnsInOrder.get(0).getColumnName().equals("ID"));
		assertTrue(tableMetaData.getColumnsbyName().get("NAME").getDataType() == java.sql.Types.VARCHAR);
		assertTrue(tableMetaData.getColumnsbyName().get("CLOB_VALUE").getDataType() == java.sql.Types.CLOB);
		assertTrue(tableMetaData.getKeyNames().contains("ID"));
		assertTrue(tableMetaData.getKeyNames().size() == 1);
	}
	
	@Test
	public void testCRUDOnTestTable() {	
		jdbcTemplate.setDataSource(datasource);
		String tableToGet = "TEST_TABLE";
		JDBCTableMetaData tableMetaData = getTableMetaData(tableToGet);
		JDBCAdHocTableUtility adHocUtility = new JDBCAdHocTableUtility(tableMetaData, jdbcTemplate, dateFormat, timeFormat);
					
		Properties testData = new Properties();
		testData.setProperty("NAME", "Testing...");
		testData.setProperty("BIGTEXT_VALUE", "Big Text Goes Here");
		testData.setProperty("CLOB_VALUE", "Bigger Text Goes Here...");
		
		testData.setProperty("DOUBLE_VALUE", "12345.6789");
		testData.setProperty("INT_VALUE", "12345");
		testData.setProperty("BIGINT_VALUE", "64123456789");
		testData.setProperty("DECIMAL_VALUE", "13.25");

		testData.setProperty("DATE_VALUE", "04/11/1967");
		testData.setProperty("TIME_VALUE", "12:34:56");
		testData.setProperty("DATETIME_VALUE", "04/11/1967 12:34:56");
		
		Properties keys = adHocUtility.insert(testData);
		logger.debug(keys.toString());
		assertEquals(keys.size(), 1);
		String key1 = keys.getProperty("ID");

		ArrayList<Properties> savedTestData = adHocUtility.select(keys, null, null);
		
		assertNotNull(savedTestData);
		assertEquals(savedTestData.size(), 1);
		assertEquals(savedTestData.get(0).getProperty("NAME"), "Testing...");
				
		testData.setProperty("NAME", "123");

		keys = adHocUtility.insert(testData);
		logger.debug(keys.toString());
		assertEquals(keys.size(), 1);
		String key2 = keys.getProperty("ID");
		
		savedTestData = adHocUtility.select(keys, null, null);
		assertNotNull(savedTestData);
		assertEquals(savedTestData.size(), 1);
		assertEquals(savedTestData.get(0).getProperty("NAME"), "123");

		assertEquals(savedTestData.get(0).getProperty("INT_VALUE"), "12345");
		savedTestData.get(0).setProperty("INT_VALUE", "987654");
		
		adHocUtility.update(savedTestData.get(0));

		savedTestData = adHocUtility.select(keys, null, null);
		assertEquals(savedTestData.get(0).getProperty("INT_VALUE"), "987654");
		
		savedTestData = adHocUtility.select(null, null, null);
		logger.debug(savedTestData.toString());			

		assertNotNull(savedTestData);
		assertEquals(savedTestData.size(), 2);

		Properties deleteKeys = new Properties();
		deleteKeys.setProperty("ID", key1);
		
		adHocUtility.delete(deleteKeys);

		savedTestData = adHocUtility.select(null, null, null);
		logger.debug(savedTestData.toString());			

		assertNotNull(savedTestData);
		assertEquals(savedTestData.size(), 1);
		assertEquals(savedTestData.get(0).getProperty("ID"), key2);
	}

	@Test
	public void testOrderBySQLGeneration() {
		JDBCTableMetaData tableMetaData = getTableMetaData("RANDOM_NAMES");
		JDBCAdHocTableUtility adHocUtility = new JDBCAdHocTableUtility(tableMetaData, jdbcTemplate, dateFormat, timeFormat);

		OrderByClause orderBy = new OrderByClause();
		orderBy.addOrderBy("LAST", OrderByDirection.ASCENDING);

		String testQuery = adHocUtility.buildSelectStatement(null, orderBy, null, null);
		logger.debug(testQuery);
		
		assertEquals(testQuery, "select * from `RANDOM_NAMES` order by LAST asc");
	}
	
	@Test
	public void testOrderBy() {
		
		JDBCTableMetaData tableMetaData = getTableMetaData("RANDOM_NAMES");
		JDBCAdHocTableUtility adHocUtility = new JDBCAdHocTableUtility(tableMetaData, jdbcTemplate, dateFormat, timeFormat);

		OrderByClause orderBy = new OrderByClause();
		orderBy.addOrderBy("LAST", OrderByDirection.ASCENDING);

		ArrayList<Properties> selectedData = adHocUtility.select(null, orderBy, null);
		logger.debug(selectedData.toString());
			
		assertEquals(selectedData.size(), 1000);
		
		Properties firstRecord = selectedData.get(0);
		
		assertNotNull(firstRecord);
		assertNotNull(firstRecord.getProperty("FIRST"), "Parker");
		assertNotNull(firstRecord.getProperty("LAST"), "Abbott");
		assertNotNull(firstRecord.getProperty("CONTROL_INDEX"), "454");

		Properties lastRecord = selectedData.get(selectedData.size()-1);
		
		assertNotNull(lastRecord);
		assertNotNull(lastRecord.getProperty("FIRST"), "Cesar ");
		assertNotNull(lastRecord.getProperty("LAST"), "Zavala");
		assertNotNull(lastRecord.getProperty("CONTROL_INDEX"), "328");
	}

	@Test
	public void testFilterSQLGeneration() {
		JDBCTableMetaData tableMetaData = getTableMetaData("RANDOM_NAMES");
		JDBCAdHocTableUtility adHocUtility = new JDBCAdHocTableUtility(tableMetaData, jdbcTemplate, dateFormat, timeFormat);

		FilterWrapper filter = new FilterWrapper("LAST like 'Ander%'");
		
		String testQuery = adHocUtility.buildSelectStatement(null, null, filter, null);
		logger.debug(testQuery);

		assertEquals(testQuery, "select z.* from (select * from `RANDOM_NAMES`) z where LAST like 'Ander%'");
	}

	@Test
	public void testFilter() {
		JDBCTableMetaData tableMetaData = getTableMetaData("RANDOM_NAMES");
		JDBCAdHocTableUtility adHocUtility = new JDBCAdHocTableUtility(tableMetaData, jdbcTemplate, dateFormat, timeFormat);

		FilterWrapper filter = new FilterWrapper("LAST like 'Ander%'");
		
		ArrayList<Properties> selectedData = adHocUtility.select(null, null, filter);

		assertEquals(selectedData.size(), 5);
		
		for (Properties record : selectedData) {
			assertNotNull(record);
			assertNotNull(record.getProperty("LAST"), "Andersen");
		}
	}
	
	@Test
	public void testPaginationSQLGenerationLimitOffset() {
		JDBCTableMetaData tableMetaData = getTableMetaData("RANDOM_NAMES");
		JDBCAdHocTableUtility adHocUtility = new JDBCAdHocTableUtility(tableMetaData, jdbcTemplate, dateFormat, timeFormat);

		PaginationWrapper pager = new PaginationWrapper(0, 100, PaginationWrapper.PagingMethod.LIMIT_OFFSET);
		String testQuery = adHocUtility.buildSelectStatement(null, null, pager, null);
		logger.debug(testQuery);

		assertEquals(testQuery, "select z.* from (select * from `RANDOM_NAMES`) z limit 100 offset 0");		
	}

	@Test
	public void testPaginationSQLGenerationRowNum() {
		JDBCTableMetaData tableMetaData = getTableMetaData("RANDOM_NAMES");
		JDBCAdHocTableUtility adHocUtility = new JDBCAdHocTableUtility(tableMetaData, jdbcTemplate, dateFormat, timeFormat);

		PaginationWrapper pager = new PaginationWrapper(0, 100, PaginationWrapper.PagingMethod.ROWNUM);
		String testQuery = adHocUtility.buildSelectStatement(null, null, pager, null);
		logger.debug(testQuery);

		assertEquals(testQuery, "select z.*, ROW_NUMBER() as xxx_rownumber_xxx from (select * from `RANDOM_NAMES`) z where xxx_rownumber_xxx between 1 and 100");		
	}

	@Test
	public void testPagination() {
		JDBCTableMetaData tableMetaData = getTableMetaData("RANDOM_NAMES");
		JDBCAdHocTableUtility adHocUtility = new JDBCAdHocTableUtility(tableMetaData, jdbcTemplate, dateFormat, timeFormat);

		int page = 0;
		int records = 0;
		boolean gotData = false;
		do {
			gotData = false;
			PaginationWrapper pager = new PaginationWrapper(page, 100, PaginationWrapper.PagingMethod.LIMIT_OFFSET);
			ArrayList<Properties> selectedData = adHocUtility.select(null, null, pager);

			assertNotNull(selectedData);
			int recordsRetrieved = selectedData.size();
			if (recordsRetrieved > 0) {
				page++;
				records += recordsRetrieved;
				gotData = true;
			}
		} while (gotData && records < 1002);
		
		assertEquals(records, 1000);		
		assertEquals(page, 10);
	}

	@Test
	public void testFilteredPaginationSQLGeneration() {
		JDBCTableMetaData tableMetaData = getTableMetaData("RANDOM_NAMES");
		JDBCAdHocTableUtility adHocUtility = new JDBCAdHocTableUtility(tableMetaData, jdbcTemplate, dateFormat, timeFormat);

		MultiQueryWrapper multiWrapper = new MultiQueryWrapper();
		FilterWrapper filter = new FilterWrapper("LAST like 'B%'");
		PaginationWrapper pager = new PaginationWrapper(0, 25, PaginationWrapper.PagingMethod.LIMIT_OFFSET);
		multiWrapper.addWrapper(filter);
		multiWrapper.addWrapper(pager);

		String testQuery = adHocUtility.buildSelectStatement(null, null, multiWrapper, null);
		logger.debug(testQuery);

		assertEquals(testQuery, "select z2.* from (select z1.* from (select * from `RANDOM_NAMES`) z1 where LAST like 'B%') z2 limit 25 offset 0");
	}//98

	@Test
	public void testFilteredPagination() {
		JDBCTableMetaData tableMetaData = getTableMetaData("RANDOM_NAMES");
		JDBCAdHocTableUtility adHocUtility = new JDBCAdHocTableUtility(tableMetaData, jdbcTemplate, dateFormat, timeFormat);

		int page = 0;
		int records = 0;
		boolean gotData = false;
		do {
			gotData = false;
			MultiQueryWrapper multiWrapper = new MultiQueryWrapper();
			FilterWrapper filter = new FilterWrapper("LAST like 'B%'");
			PaginationWrapper pager = new PaginationWrapper(page, 25, PaginationWrapper.PagingMethod.LIMIT_OFFSET);
			multiWrapper.addWrapper(filter);
			multiWrapper.addWrapper(pager);
			
			ArrayList<Properties> selectedData = adHocUtility.select(null, null, multiWrapper);

			assertNotNull(selectedData);
			int recordsRetrieved = selectedData.size();
			if (recordsRetrieved > 0) {
				page++;
				records += recordsRetrieved;
				gotData = true;
			}
		} while (gotData && records < 1002);
		
		assertEquals(records, 98);		
		assertEquals(page, 4);
	}
}
