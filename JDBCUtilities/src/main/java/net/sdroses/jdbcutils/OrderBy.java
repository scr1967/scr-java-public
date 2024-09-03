/**
 * Created on Aug 30, 2024
 */
package net.sdroses.jdbcutils;

/**
 * This represents an entry in an SQL ORDER BY clause
 * 
 * @author Steve Rose
 */
public class OrderBy {
	/**
	 * Column name matching a column in the select clause. Could be a numeric value if the RDBMS supports column numbers in the ORDER BY
	 */
	public String column;
	/**
	 * {@link OrderByDirection#ASCENDING ASCENDING} or {@link OrderByDirection#DESCENDING DESCENDING}
	 */
	public OrderByDirection direction;
	
	/**
	 * Constructor
	 * @param col String Column name matching a column in the select clause. Could be a numeric value if the RDBMS supports column numbers in the ORDER BY
	 * @param dir {@link OrderByDirection} {@link OrderByDirection#ASCENDING ASCENDING} or {@link OrderByDirection#DESCENDING DESCENDING}
	 */
	public OrderBy(String col, OrderByDirection dir) {
		column = col;
		direction = dir;
	}

}
