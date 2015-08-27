package edu.kit.aifb.cumulus.datasource.impl;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.AbstractIterator;

import edu.kit.aifb.cumulus.datasource.serializer.Serializer;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.MapDAO;
import edu.kit.aifb.cumulus.log.Log;

/**
 * A map DAO that operates on a Cassandra table.
 * 
 * @author Sebastian Schmidt
 * @since 1.0.0
 * 
 * @param <K> The key type.
 * @param <V> The value type.
 */
class Cassandra2xDefaultMapDAO<K, V> implements MapDAO<K, V> {

	private final Log _log = new Log(LoggerFactory.getLogger(getClass()));

	private final Session _session;
	private final String _tableName;
	private final boolean _bidirectional;
	private final Serializer<K> _keySerializer;
	private final Serializer<V> _valueSerializer;
	private V _defaultValue;
	private K _defaultKey;
	private final int _ttl;

	private PreparedStatement _insertStatement;
	private PreparedStatement _deleteStatement;
	private PreparedStatement _getValueStatement;
	private PreparedStatement _getKeyStatement;
	private PreparedStatement _getAllStatement;

	/**
	 * Creates a new simple DAO.
	 * 
	 * @param session The connection to Cassandra.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the values.
	 */
	public Cassandra2xDefaultMapDAO(final Session session, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
		this(session, "simple_dao", keySerializer, valueSerializer);
	}

	/**
	 * Creates a new simple DAO.
	 * 
	 * @param session The connection to Cassandra.
	 * @param tableName The name of the table that the DAO should operate on.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the values.
	 */
	public Cassandra2xDefaultMapDAO(final Session session, final String tableName, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
		this(session, tableName, false, keySerializer, valueSerializer);
	}

	/**
	 * Creates a new simple DAO.
	 * 
	 * @param session The connection to Cassandra.
	 * @param tableName The name of the table that the DAO should operate on.
	 * @param bidirectional True if the DAO should allow reverse lookups. Note that reverse lookups for values > 64KB are not possible.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the values.
	 */
	public Cassandra2xDefaultMapDAO(final Session session, final String tableName, final boolean bidirectional, final Serializer<K> keySerializer,
			final Serializer<V> valueSerializer) {
		this(session, tableName, false, -1, keySerializer, valueSerializer);
	}

	/**
	 * Creates a new simple DAO.
	 * 
	 * @param session The connection to Cassandra.
	 * @param tableName The name of the table that the DAO should operate on.
	 * @param bidirectional True if the DAO should allow reverse lookups. Note that reverse lookups for values > 64KB are not possible.
	 * @param ttl The TTL for the entries.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the values.
	 */
	public Cassandra2xDefaultMapDAO(
			final Session session, final String tableName, final boolean bidirectional,
			final int ttl, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
		_session = session;
		_tableName = tableName;
		_bidirectional = bidirectional;
		_keySerializer = keySerializer;
		_valueSerializer = valueSerializer;
		_ttl = ttl;
	}

	@Override
	public void createRequiredSchemaEntities() throws DataAccessLayerException {
		_session.execute("CREATE TABLE IF NOT EXISTS " + _tableName + " (key BLOB, value BLOB, PRIMARY KEY (key))"
				+ " WITH compaction = {'class': 'LeveledCompactionStrategy'}"
				+ " AND compression = {'sstable_compression' : 'SnappyCompressor'}");

		if (_bidirectional) {
			_session.execute("CREATE INDEX IF NOT EXISTS " + _tableName + "_value_index ON " + _tableName + " (value)");
			_getKeyStatement = _session.prepare("SELECT key FROM " + _tableName + " WHERE value = ?");
		}

		if (_ttl != -1) {
			_insertStatement = _session.prepare("INSERT INTO " + _tableName + " (key, value) VALUES (?, ?) USING TTL " + _ttl);
		} else {
			_insertStatement = _session.prepare("INSERT INTO " + _tableName + " (key, value) VALUES (?, ?)");
		}

		_deleteStatement = _session.prepare("DELETE FROM " + _tableName + " WHERE key = ?");
		_getValueStatement = _session.prepare("SELECT value FROM " + _tableName + " WHERE key = ?");
		_getAllStatement = _session.prepare("SELECT key, value FROM " + _tableName);
	}

	@Override
	public boolean contains(final K key) {
		ByteBuffer serializedKey = _keySerializer.serialize(key);

		BoundStatement containsStatement = _getValueStatement.bind(serializedKey);
		return _session.execute(containsStatement).one() != null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void delete(final K... keys) {
		if (keys == null || keys.length == 0) {
			return;
		}

		BatchStatement batchStatement = new BatchStatement();

		for (K key : keys) {
			if (key != null) {
				ByteBuffer serializedKey = _keySerializer.serialize(key);

				BoundStatement deleteStatement = _deleteStatement.bind(serializedKey);
				batchStatement.add(deleteStatement);
			}
		}

		_session.execute(batchStatement);
	}

	@Override
	public V get(final K key) {
		ByteBuffer serializedKey = _keySerializer.serialize(key);

		BoundStatement getValueStatement = _getValueStatement.bind(serializedKey);
		Row result = _session.execute(getValueStatement).one();

		if (result == null) {
			return _defaultValue;
		} else {
			return _valueSerializer.deserialize(result.getBytesUnsafe(0));
		}
	}

	@Override
	public K getKey(final V value) {
		if (!_bidirectional) {
			throw new IllegalStateException("Map is not bidirectional");
		}

		ByteBuffer serializedValue = _valueSerializer.serialize(value);

		BoundStatement getKeyStatement = _getKeyStatement.bind(serializedValue);
		Row result = _session.execute(getKeyStatement).one();

		if (result == null) {
			return _defaultKey;
		} else {
			return _keySerializer.deserialize(result.getBytesUnsafe(0));
		}
	}

	@Override
	public Iterator<K> keyIterator() {
		BoundStatement getAllStatement = _getAllStatement.bind();
		final Iterator<Row> resultIterator = _session.execute(getAllStatement).iterator();

		return new AbstractIterator<K>() {
			@Override
			protected K computeNext() {
				if (!resultIterator.hasNext()) {
					return endOfData();
				} else {
					return _keySerializer.deserialize(resultIterator.next().getBytesUnsafe(0));
				}
			}
		};
	}

//	@Override
//	public Iterator<K> keyIterator(final V value) {
//		if (!_bidirectional) {
//			throw new IllegalStateException("DAO is not bidirectional");
//		}
//
//		BoundStatement getKeyStatement = _getKeyStatement.bind(_valueSerializer.serialize(value));
//		final Iterator<Row> resultIterator = _session.execute(getKeyStatement).iterator();
//
//		return new AbstractIterator<K>() {
//			@Override
//			protected K computeNext() {
//				if (!resultIterator.hasNext()) {
//					return endOfData();
//				} else {
//					return _keySerializer.deserialize(resultIterator.next().getBytesUnsafe(0));
//				}
//			}
//		};
//	}

	@Override
	public Set<K> keySet() {

		Set<K> keys = new HashSet<K>();

		for (Iterator<K> iter = keyIterator(); iter.hasNext();) {
			keys.add(iter.next());
		}

		return keys;
	}

	/**
	 * Returns a {@link BoundStatement} to insert the given key/value pair.
	 * @param key The key.
	 * @param value The value.
	 * @return A BoundStatement to insert the given key/value pair.
	 */
	private BoundStatement getInsertStatement(final K key, final V value) {
		BoundStatement insertStatement = _insertStatement.bind();
		insertStatement.setBytesUnsafe(0, _keySerializer.serialize(key));
		insertStatement.setBytesUnsafe(1, _valueSerializer.serialize(value));

		return insertStatement;
	}

	@Override
	public void set(final K key, final V value) {
		BoundStatement insertStatement = getInsertStatement(key, value);
		_session.execute(insertStatement);
	}

	@Override
	public void setAll(final Map<K, V> pairs) {
		if (pairs.size() == 0) {
			return;
		}

		BatchStatement batchStatement = new BatchStatement();

		for (Map.Entry<K, V> entry : pairs.entrySet()) {
			batchStatement.add(getInsertStatement(entry.getKey(), entry.getValue()));
		}

		try {
			_session.execute(batchStatement);
		} catch (Exception e) {
			_log.error("failed to insert batch of " + pairs.size() + " dictionary entries", e);
		}
	}

	@Override
	public void setDefaultValue(final V defaultValue) {
		_defaultValue = defaultValue;
	}
}