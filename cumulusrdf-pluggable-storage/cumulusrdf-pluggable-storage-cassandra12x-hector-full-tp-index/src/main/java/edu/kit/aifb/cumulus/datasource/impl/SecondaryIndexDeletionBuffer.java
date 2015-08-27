package edu.kit.aifb.cumulus.datasource.impl;

import java.util.HashSet;
import java.util.Set;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.mutation.Mutator;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.TripleIndexDAO;

/**
 * A buffer for secondary index deletion checks.
 *  
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 * @since 1.0.1
 */
class SecondaryIndexDeletionBuffer {
	private final Set<SecondaryIndexDeletionCandidate> _candidates;
	private final int _capacity;

	/**
	 * Creates a new SI deletion buffer. The capacity is not treated as hard limit. The buffer might grow infinite.
	 * It is only flushed if the {@link #flushIfFull(Mutator, String, Object, Serializer)} or {@link #flush(Mutator, String, Object, Serializer)} method is called.
	 * 
	 * @param capacity The approximate capacity of the buffer.
	 */
	SecondaryIndexDeletionBuffer(final int capacity) {
		_capacity = capacity;
		_candidates = new HashSet<SecondaryIndexDeletionCandidate>(capacity);
	}

	/**
	 * Adds a new element to the buffer. If the buffer already contains an element equal to the new one,
	 * it is ignored.
	 * 
	 * @param candidate The deletion candidate.
	 */
	void add(final SecondaryIndexDeletionCandidate candidate) {
		_candidates.add(candidate);
	}

	/**
	 * Flushes the buffer if it is full.
	 * 
	 * @param mutator The mutator to fill with deletions.
	 * @param columnFamilyName The column (name) family to delete from.
	 * @param column The column to delete from.
	 * @param serializer The serializer to use for the column.
	 * @param dao the rdf index data access object.
	 * @param <T> The type of the column key.
	 * @return True if the buffer was flushed and at least one element was checked for deletion, false otherwise.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	<T> boolean flushIfFull(
			final Mutator<byte[]> mutator, 
			final String columnFamilyName, 
			final T column, 
			final Serializer<T> serializer,
			final TripleIndexDAO dao) throws DataAccessLayerException {
		if (_candidates.size() >= _capacity) {
			return flush(mutator, columnFamilyName, column, serializer, dao);
		} else {
			return false;
		}
	}
	
	/**
	 * Flushes the buffer.
	 * 
	 * @param mutator The mutator to fill with deletions.
	 * @param columnFamilyName The column family (name) to delete from.
	 * @param column The column to delete from.
	 * @param serializer The serializer to use for the column.
	 * @param dao the rdf index data access object.
	 * @param <T> The type of the column key.
	 * @return True if the buffer was flushed and at least one element was checked for deletion, false otherwise.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	<T> boolean flush(
			final Mutator<byte[]> mutator, 
			final String columnFamilyName, 
			final T column, 
			final Serializer<T> serializer,
			final TripleIndexDAO dao) throws DataAccessLayerException {
		if (_candidates.size() == 0) {
			return false;
		}
		
		for (SecondaryIndexDeletionCandidate candidate : _candidates) {
			if (!dao.query(candidate.getQuery(), 1).hasNext()) {
				mutator.addDeletion(candidate.getRow(), columnFamilyName, column, serializer);
			}
		}
		
		return true;
	}
}