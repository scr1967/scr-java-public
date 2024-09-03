/**
 * Created on Aug 30, 2024
 */
package net.sdroses.jdbcutils;

import java.util.ArrayList;

/**
 * Iteratively wrap an SQL query in multiple wrappers. Useful when filtering and then paginating a canned query.
 * 
 * @author Steve Rose
 */
public class MultiQueryWrapper implements QueryWrapper {

	/**
	 * List of wrappers to apply, starting with the inner-most wrapper and working outward 
	 */
	private ArrayList<QueryWrapper> wrappers = new ArrayList<QueryWrapper>(); 
	
	/**
	 * Add an outermost wrapper to the list of wrappers. Each wrapper added becomes the outer-most wrapper.
	 * @param wrapper
	 */
	public void addWrapper(QueryWrapper wrapper) {
		wrappers.add(wrapper);
	}
	
	/**
	 * Iteratively wrap an SQL query in multiple wrappers
	 * @param baseQuery String original query
	 * @param wrapAlias alias for the sub-query. This must be a valid table alias (alpha-numeric, no spaces). 
	 * This appends a level to the wrapAlias to avoid conflicts 
	 * @return wrapped query
	 */
	@Override
	public String wrap(String baseQuery, String wrapAlias) {
		
		String wrappedQuery = baseQuery;
		int level = 0;
		for (QueryWrapper wrapper : wrappers) {
			wrappedQuery = wrapper.wrap(wrappedQuery, wrapAlias + (++level));
		}
		
		return wrappedQuery;
	}

}
