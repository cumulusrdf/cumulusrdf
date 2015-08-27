package edu.kit.aifb.cumulus.datasource.impl;

import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.BYTE_SERIALIZER;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.COMPOSITE_SERIALIZER;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.C_COL;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.INCLUDE_ALL_COMPOSITE_HIGHER_BOUND;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.INCLUDE_ALL_COMPOSITE_LOWER_BOUND;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.PC_COL;

import java.nio.ByteBuffer;
import java.util.Arrays;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.RangeSlicesIterator;
import me.prettyprint.cassandra.service.template.SliceFilter;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import com.google.common.collect.AbstractIterator;

import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;

/**
 * An iterator to iterate over the results of a (? p ? c) or (? ? ? c) query.
 * 
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CAndPCSlicesQueryIterator extends AbstractIterator<byte[][]> {
	private final int _limit;

	/**
	 * True if this iterates over the result of a PC query, false if it is a C
	 * query.
	 */
	private final boolean _isPC;
	private int _returned;

	private final String _cf;
	private final Keyspace _keyspace;

	private byte[] _subject;
	private byte[] _predicate;
	private byte[] _object;
	private byte[] _context;

	private final ITopLevelDictionary _dictionary;
	
	private final RangeSlicesIterator<byte[], Composite, byte[]> _rows;
	private ColumnSliceIterator<byte[], Composite, byte[]> _columns;

	private static final SliceFilter<HColumn<Composite, byte[]>> PC_FILTER = new SliceFilter<HColumn<Composite, byte[]>>() {
		@Override
		public boolean accept(final HColumn<Composite, byte[]> column) {
			return !Arrays.equals(BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) column.getName().get(0)), (byte[]) PC_COL.get(0));
		}
	};
	
	private static final SliceFilter<HColumn<Composite, byte[]>> C_FILTER = new SliceFilter<HColumn<Composite, byte[]>>() {
		@Override
		public boolean accept(final HColumn<Composite, byte[]> column) {
			return !Arrays.equals(BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) column.getName().get(0)), (byte[]) C_COL.get(0));
		}
	};
	
	
	/**
	 * Creates a new iterator iterating over the results of the given query.
	 * 
	 * @param query The query.
	 * @param limit The maximum amount of results to return.
	 * @param cf The column family to query.
	 * @param keyspace The keyspace to use.
	 * @param dictionary the CumulusRDF dictionary.
	 * @param isPC True, if this is a PC query, false, if this is a C query.
	 */
	CAndPCSlicesQueryIterator(
			final RangeSlicesQuery<byte[], Composite, byte[]> query,
			final int limit, 
			final String cf, 
			final Keyspace keyspace, 
			final ITopLevelDictionary dictionary,
			final boolean isPC) {
		_rows = new RangeSlicesIterator<byte[], Composite, byte[]>(query, new byte[0], new byte[0]);
		_limit = limit;
		_cf = cf;
		_keyspace = keyspace;
		_dictionary = dictionary;
		_isPC = isPC;
	}

	@Override
	protected byte[][] computeNext() {
		if (_columns == null || !_columns.hasNext()) {
			if (!_rows.hasNext()) {
				return endOfData();
			}

			final Row<byte[], Composite, byte[]> row = _rows.next();
			final byte [][] ids = _dictionary.decompose(row.getKey());

			if (_isPC) {
				_subject = ids[0];
				_predicate = ids[1];
				_context = ids[2];
			} else {
				_object = ids[0];
				_context = ids[1];
			}

			_columns = new ColumnSliceIterator<byte[], Composite, byte[]>(
					HFactory.createSliceQuery(_keyspace, BYTE_SERIALIZER, COMPOSITE_SERIALIZER, BYTE_SERIALIZER)
						.setColumnFamily(_cf)
						.setRange(INCLUDE_ALL_COMPOSITE_LOWER_BOUND, INCLUDE_ALL_COMPOSITE_HIGHER_BOUND, false, _limit)
						.setKey(row.getKey()),
					INCLUDE_ALL_COMPOSITE_LOWER_BOUND, 
					INCLUDE_ALL_COMPOSITE_HIGHER_BOUND, 
					false);
			_columns.setFilter(_isPC ? PC_FILTER : C_FILTER);
		}

		if (_columns != null && _columns.hasNext() && _limit > _returned) {
			_returned++;

			final Composite next = _columns.next().getName();

			if (_isPC) {
				return new byte[][] {
						_subject, 
						_predicate, 
						BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) next.get(0)), 
						_context};
			} else {
				return new byte[][] {
						BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) next.get(1)), 
						BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) next.get(0)), 
						_object, 
						_context};
			}
		} else {
			return endOfData();
		}
	}
}