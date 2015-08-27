package edu.kit.aifb.cumulus.datasource.impl;

import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.BYTE_SERIALIZER;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.COMPOSITE_SERIALIZER;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.DONT_INCLUDE_PREDICATE_COLUMN;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.INCLUDE_ALL_COMPOSITE_HIGHER_BOUND;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.INCLUDE_ALL_COMPOSITE_LOWER_BOUND;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.IndexedSlicesIterator;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

import com.google.common.collect.AbstractIterator;

import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;

/**
 * An iterator to iterate over the triples for pattern "?s p ?o" in the POS
 * index.
 * 
 * @author Andreas Wagner
 */
@SuppressWarnings("deprecation")
public class POSSlicesQueryIterator extends AbstractIterator<byte[][]> {
	private final int _limit;
	private int _returned;

	private final ITopLevelDictionary _dictionary;
	
	private final String _cf;
	private final Keyspace _keyspace;

	private byte[] _predicate, _object;

	private IndexedSlicesIterator<byte[], Composite, byte[]> _rows;
	private ColumnSliceIterator<byte[], Composite, byte[]> _colIter;

	/**
	 * Builds a new iterator with the given data.
	 * 
	 * @param dictionary the store dictionary.
	 * @param isq the indexed slice query.
	 * @param limit the result limit.
	 * @param cf the column family name.
	 * @param keyspace the keyspace.
	 */
	POSSlicesQueryIterator(
			final ITopLevelDictionary dictionary,
			final IndexedSlicesQuery<byte[], Composite, byte[]> isq,
			final int limit, 
			final String cf, 
			final Keyspace keyspace) {
		_dictionary = dictionary;
		_limit = limit;
		_cf = cf;
		_keyspace = keyspace;
		_rows = new IndexedSlicesIterator<byte[], Composite, byte[]>(isq, new byte[0]);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected byte[][] computeNext() {

		if (_colIter == null || !_colIter.hasNext()) {

			if (!_rows.hasNext()) {
				return endOfData();
			}

			final Row<byte[], Composite, byte[]> row = _rows.next();

			final byte[][] po = _dictionary.decompose(row.getKey());
			_predicate = po[0];
			_object = po[1];

			final SliceQuery<byte[], Composite, byte[]> sq = HFactory.createSliceQuery(_keyspace, BYTE_SERIALIZER, COMPOSITE_SERIALIZER, BYTE_SERIALIZER)
					.setColumnFamily(_cf)
					.setRange(INCLUDE_ALL_COMPOSITE_LOWER_BOUND, INCLUDE_ALL_COMPOSITE_HIGHER_BOUND, false, Integer.MAX_VALUE)
					.setKey(row.getKey());

			_colIter = new ColumnSliceIterator<byte[], Composite, byte[]>(sq, INCLUDE_ALL_COMPOSITE_LOWER_BOUND, INCLUDE_ALL_COMPOSITE_HIGHER_BOUND, false)
					.setFilter(DONT_INCLUDE_PREDICATE_COLUMN);
		}

		if (_colIter != null && _colIter.hasNext() && _limit > _returned) {
			_returned++;

			final Composite next = _colIter.next().getName();

			if (next.size() == 2) {
				return new byte[][] {
						BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) next.get(0)),
						_predicate,
						_object,
						BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) next.get(1)) };
			} else {
				return new byte[][] {
						BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) next.get(0)),
						_predicate,
						_object };
			}
		} else {
			return endOfData();
		}
	}
}