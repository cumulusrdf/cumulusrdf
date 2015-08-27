package edu.kit.aifb.cumulus.datasource.impl;

import java.util.Arrays;

/**
 * Used to save an entry in a secondary index that may be deleted.
 * 
 * @author Sebastian Schmidt
 * @since 1.0.1
 */
class SecondaryIndexDeletionCandidate {
	private final byte[] _row;
	private final byte[][] _query;
	
	/**
	 * Creates a new object.
	 * @param row The row to check for deletion.
	 * @param query The query used to check for deletion.
	 */
	SecondaryIndexDeletionCandidate(final byte[] row, final byte[][] query) {
		_row = row;
		_query = query;
	}

	/**
	 * Returns the row to check for deletion.
	 * @return The row to check for deletion.
	 */
	byte[] getRow() {
		return _row;
	}

	/**
	 * Returns the query used to check for deletion.
	 * @return The query used to check for deletion.
	 */
	byte[][] getQuery() {
		return _query;
	}
	
	@Override
	public int hashCode() {
		int hash = Arrays.hashCode(_row);
		
		for (int i = 0; i < _query.length; i++) {
			// Do it like they do in openjdk
			hash = hash * 31 + Arrays.hashCode(_query[i]);
		}
		
		return hash;
	}
	
	@Override
	public boolean equals(final Object o) {
		if (o instanceof SecondaryIndexDeletionCandidate) {
			final SecondaryIndexDeletionCandidate s = (SecondaryIndexDeletionCandidate) o;
			return Arrays.equals(s._row, _row) && Arrays.deepEquals(s._query, _query);
		} else {
			return false;
		}
	}
}