/**
 * Created on Aug 30, 2024
 */
package net.sdroses.jdbcutils;

/**
 * <p>Add a filter (where clause) to the end of an existing query, 
 * useful for filtering canned queries that might already have 
 * complex where clauses making modifications to the existing 
 * where clause difficult.
 * <h1>Example</h1>
 * <p>Select all records from mytable with null category or group and has name that starts with J. 
 * Assume that the query for null category or group is canned and not reasonably modifiable to filter on J names.
 * <code>
 * <p>Wrapper filter = new FilterWrapper("name like 'J%'");
 * <p>filter.wrap("select * from mytable where category is null || group is null", "z");
 * <h2>Result</h2>
 * <code>
 * <p>select z.* from (select * from mytable where category is null || group is null) z where name like 'J%'");
 * 
 * @author Steve Rose
 */
public class FilterWrapper implements QueryWrapper {

	/**
	 * Filter condition
	 */
	private String filter;
	
	/**
	 * Constructor
	 * 
	 * @param filter
	 */
	public FilterWrapper(String filter) {
		this.filter = filter;
	}
	
	/**
	 * <p>Wrap an SQL query to turn it into a sub-query in the form: 
	 * <p><code>select * from ( {{baseQuery}} ) {{wrapAlias}}</code>
	 * @param baseQuery String original query
	 * @param wrapAlias alias for the sub-query. This must be a valid table alias (alpha-numeric, no spaces) 
	 * @return wrapped query
	 */
	@Override
	public String wrap(String baseQuery, String wrapAlias) {
		return "select " + wrapAlias + ".* from (" + baseQuery + ") " + wrapAlias + " where " + filter;
	}
}
