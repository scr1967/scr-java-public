/**
 * Created on Aug 30, 2024
 */
package net.sdroses.jdbcutils.order;

/**
 * Enum representing the Ascending and Descending states for an ORDER BY clause
 * 
 * @author Steve Rose
 */
public enum OrderByDirection {
	
	ASCENDING,
	DESCENDING;
	
	/**
	 * asc
	 */
	private static final String ASC = "asc";
	
	/**
	 * desc
	 */
	private static final String DESC = "desc";

	/**
	 * Makes an OrderByDirection based on the string values {@link #ASC} and {@link #DESC}, case insensitive.
	 * @param s
	 * @return
	 */
	public static OrderByDirection make(String s) {
		if (ASC.equalsIgnoreCase(s)) {
			return ASCENDING;
		} if (DESC.equalsIgnoreCase(s)) {
			return DESCENDING;
		} else {
			return null;			
		}
	}
	
	/**
	 * @return The SQL String value of the direction: {@link #ASC} or {@link #DESC}
	 */
	public String toString() {
		switch (this) {
		case ASCENDING:
			return ASC;
		case DESCENDING:
			return DESC;
		}
		
		return "";
	}
}
