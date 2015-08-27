package edu.kit.aifb.cumulus.datasource.impl;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

import edu.kit.aifb.cumulus.datasource.serializer.Serializer;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * An iterator that converts {@link Row}s to arrays of ids.
 * 
 * @author Sebastian Schmidt
 * @since 1.1
 */
public abstract class AbstractResultIterator implements Iterator<byte[][]> {
	private static final Log LOG = new Log(LoggerFactory.getLogger(AbstractResultIterator.class));

	private final ResultSetFuture _resultSetFuture;
	private Iterator<Row> _resultIterator;
	protected final Serializer<byte[]> _idSerializer = Serializer.BYTE_ARRAY_SERIALIZER;

	/**
	 * Creates a new result iterator using the given session to execute the given statement.
	 * Closes the session after usage.
	 * 
	 * @param resultSetFuture The result set to iterate through.
	 */
	public AbstractResultIterator(final ResultSetFuture resultSetFuture) {
		_resultSetFuture = resultSetFuture;
	}

	@Override
	public boolean hasNext() {
		ensureResultIteratorExists();

		if (_resultIterator != null) {
			return _resultIterator.hasNext();
		} else {
			return false;
		}
	}

	@Override
	public byte[][] next() {
		if (!hasNext()) {
			throw new NoSuchElementException("Iterator doesn't have more elements");
		}

		return convertRow(_resultIterator.next());
	}

	/**
	 * Converts a result row to an array of ids.
	 * 
	 * @param row The result row.
	 * @return The row as array of ids.
	 */
	protected abstract byte[][] convertRow(Row row);

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Not implemented!");
	}

	/**
	 * Ensures that the result iterator is not null.
	 */
	public void ensureResultIteratorExists() {
		if (_resultIterator == null) {
			// Only synchronized if necessary.
			synchronized (this) {
				try {
					_resultIterator = _resultSetFuture.get().iterator();
				} catch (InterruptedException e) {
					LOG.error(MessageCatalog._00106_ERROR_FETCHING_QUERY_RESULT, e);
				} catch (ExecutionException e) {
					LOG.error(MessageCatalog._00106_ERROR_FETCHING_QUERY_RESULT, e);
				}
			}
		}
	}
}