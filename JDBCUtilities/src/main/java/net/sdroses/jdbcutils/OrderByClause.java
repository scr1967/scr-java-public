/**
 * Created on Aug 30, 2024
 */
package net.sdroses.jdbcutils;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * Utility for constructing, modifying, and generating an SQL ORDER BY clause 
 * 
 * @author Steve Rose
 */
public class OrderByClause {
	/**
	 * Order By entries in the order in which they will be added to the ORDER BY clause
	 */
	ArrayList<OrderBy> orderByFields = new ArrayList<OrderBy>();
	
	/**
	 * Hashable index for searching
	 */
	Hashtable<String, OrderBy> index = new Hashtable<String, OrderBy>();
	
	/**
	 * Checks to see if any ordering has been set
	 * @return
	 */
	public boolean hasOrderByColumns() {
		return orderByFields.size() > 0;
	}
	
	/**
	 * Returns the standard SQL String value of the ordering direction for a column   
	 * @param column
	 * @return
	 */
	public String getOrderByDirection(String column) {
		if (column == null) {
			return "";
		}
		OrderBy foundField = index.get(column);
		if (foundField == null) {
			return "";
		}
		
		return foundField.direction.toString();
	}

	/**
	 * Adds a column and direction to the end of the order by list if it doesn't already exist or sets the direction if it does. 
	 * Set the direction to null to remove it
	 * @param column
	 * @param direction
	 */
	public void addOrderBy(String column, OrderByDirection direction) {
		OrderBy foundSortField = index.get(column);
		// if direction is null, we need return 
		// after removing the column if it exists.
		if (direction == null) {
			if (foundSortField != null) {
				removeOrderBy_Internal(column, foundSortField);
			}
			return;
		}
		if (foundSortField == null) {
			addOrderBy_Internal(column, new OrderBy(column, direction));
		} else {
			foundSortField.direction = direction;
		}
	}

	/**
	 * <p>Toggle the sort order between 3 states: Ascending, Descending, no sorting.
	 * 
	 * <p>If the column isn't in the list, it's added as Ascending.
	 * <p>If the column is in the list, and is Ascending, it's changed to Descending.
	 * <p>If the column is in the list, and is Descending (i.e., not Ascending) it's removed from the list.
	 * 
	 * @param column
	 */
	public void toggleSort(String column) {
		OrderBy foundSortField = index.get(column);
		if (foundSortField == null) {
			addOrderBy_Internal(column, new OrderBy(column, OrderByDirection.ASCENDING));
		} else if (foundSortField.direction == OrderByDirection.ASCENDING) {
			foundSortField.direction = OrderByDirection.DESCENDING;
		} else {
			removeOrderBy_Internal(column, foundSortField);
		}
	}

	/**
	 * Internal function for removing an OrderBy for a column, which modifies the internal data structures.
	 * @param column
	 * @param existingOrderBy
	 */
	private void removeOrderBy_Internal(String column, OrderBy existingOrderBy) {
		orderByFields.remove(existingOrderBy);
		index.remove(column);
	}

	/**
	 * Internal function for adding an OrderBy for a column, which modifies the internal data structures.
	 * @param column
	 * @param existingOrderBy
	 */
	private void addOrderBy_Internal(String column, OrderBy existingOrderBy) {
		orderByFields.add(existingOrderBy);
		index.put(column, existingOrderBy);
	}
	
	/**
	 * Generates the SQL Order By clause
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (OrderBy sortBy : orderByFields) {
			if (first) {
				sb.append("order by ");
				first = false;
			} else {
				sb.append(", ");
			}
			sb.append(sortBy.column + " " + sortBy.direction);
		}
		
		return sb.toString();
	}
}
