/**
 * Created on Aug 30, 2024
 */
package net.sdroses.jdbcutils;

/**
 * <p>Wrapps an SQL query to allow for paginated results
 * <p>Supported Pagination methods include LIMIT_OFFSET (supported by MySQL, etc), and ROW_NUMBER() (Oracle)
 * <p>Set the page (starting with 0) and the number of rows to return in the result set.
 * 
 * @author Steve Rose
 */
public class PaginationWrapper implements QueryWrapper {

	/**
	 * List of supported pagination methods
	 */
	public enum PagingMethod {
		/**
		 * Use of LIMIT and OFFSET, supported by MySQL etc
		 */
		LIMIT_OFFSET, 
		/**
		 * Use of Oracle's ROW_NUMBER() function
		 */
		ROWNUM
	}
	
	/**
	 * 0-based page #
	 */
	private int page;

	/**
	 * maximum number of rows to return
	 */
	private int rows;
	
	/**
	 * pagination method to utilize
	 */
	private PagingMethod paginationMethod;
	
	/**
	 * Constructor
	 * @param page int 0-based page #
	 * @param rows int maximum number of rows to return
	 * @param method pagination method to utilize
	 */
	public PaginationWrapper(int page, int rows, PagingMethod method) {
		super();
		this.page = page;
		this.rows = rows;
		this.paginationMethod = method;
	}

	/**
	 * <p>Wrap an SQL query to enable SQL pagination, depending on the method specified
	 * <h1>LIMIT_OFFSET
	 * <p><code>select {{wrapAlias}}.* from ( {{baseQuery}} ) {{wrapAlias}} LIMIT {{rows}} OFFSET {{rows * page}}</code>
	 * <h1>ROWNUM
	 * <p><code>select {{wrapAlias}}.*, ROW_NUMBER() as xxx_rownumber_xxx from ( {{baseQuery}} ) {{wrapAlias}} where xxx_rownumber_xxx between {{rows * page}} and {{rows * (page+1)}}</code>
	 * @param baseQuery String original query
	 * @param wrapAlias alias for the sub-query. This must be a valid table alias (alpha-numeric, no spaces) 
	 * @return wrapped query
	 */
	@Override
	public String wrap(String baseQuery, String wrapAlias) {
		return getStart(wrapAlias) + baseQuery + getEnd(wrapAlias);
	}

	/**
	 * Build first part of the pagination wrapper taking method into account
	 * @param wrapAlias
	 * @return
	 */
	private String getStart(String wrapAlias) {
		if (paginationMethod == PagingMethod.ROWNUM) {
			return "select " + wrapAlias + ".*, ROW_NUMBER() as xxx_rownumber_xxx from (";			
		}
		return "select " + wrapAlias + ".* from (";
	}

	/**
	 * Build last part of the pagination wrapper taking method into account
	 * @param wrapAlias
	 * @return
	 */
	private String getEnd(String wrapAlias) {
		if (paginationMethod == PagingMethod.ROWNUM) {
			return ") " + wrapAlias + " where xxx_rownumber_xxx between " + (page * rows + 1) + " and " + (page + 1) * rows; 
		}
		return  ") " + wrapAlias + " limit " + rows + " offset " + (page * rows);
	}
}
