/**
 * Created on Aug 30, 2024
 */
package net.sdroses.jdbcutils.wrapper;

/**
 * <p>This interface is used for wrapping an SQL query to turn it into a sub-query</p>
 * 
 * <h1>Example</h1>
 * <code>wrap("select * from mytable", "Z") --> "select Z.* from (select * from mytable) Z"
 * <p>See {@link FilterWrapper}, {@link PaginationWrapper}, {@link MultiQueryWrapper} for further examples.
 * 
 * @author Steve Rose
 */
public interface QueryWrapper {
	
	/**
	 * Wrap an SQL query to turn it into a sub-query
	 * @param baseQuery String original query
	 * @param wrapAlias alias for the sub-query. This must be a valid table alias (alpha-numeric, no spaces)
	 * @return wrapped query
	 */
	public String wrap(String baseQuery, String wrapAlias);
}
