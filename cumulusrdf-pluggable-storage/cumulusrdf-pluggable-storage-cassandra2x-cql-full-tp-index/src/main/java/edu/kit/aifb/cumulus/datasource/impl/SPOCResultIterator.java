package edu.kit.aifb.cumulus.datasource.impl;

import java.util.Arrays;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

/**
 * A {@link AbstractResultIterator ResultIterator} that converts spoc rows.
 * 
 * @author Sebastian Schmidt
 * @since 1.1
 */
public class SPOCResultIterator extends AbstractResultIterator {
	private final boolean _hasContext;

	/**
	 * Creates a new {@link SPOCResultIterator}.
	 * 
	 * @param resultSetFuture The result set to iterate through.
	 * @param hasContext True if the given result iterator should be tested for a context.
	 */
	SPOCResultIterator(final ResultSetFuture resultSetFuture, final boolean hasContext) {
		super(resultSetFuture);

		_hasContext = hasContext;
	}

	@Override
	protected byte[][] convertRow(final Row row) {
		byte[] s = _idSerializer.deserialize(row.getBytesUnsafe(0));
		byte[] p = _idSerializer.deserialize(row.getBytesUnsafe(1));
		byte[] o = _idSerializer.deserialize(row.getBytesUnsafe(2));

		if (_hasContext) {
			byte[] c = _idSerializer.deserialize(row.getBytesUnsafe(3));

			if (Arrays.equals(c, Cassandra2xConstants.EMPTY_VAL)) {
				return new byte[][] { s, p, o };
			} else {
				return new byte[][] { s, p, o, c };
			}
		} else {
			return new byte[][] { s, p, o };
		}
	}
}
