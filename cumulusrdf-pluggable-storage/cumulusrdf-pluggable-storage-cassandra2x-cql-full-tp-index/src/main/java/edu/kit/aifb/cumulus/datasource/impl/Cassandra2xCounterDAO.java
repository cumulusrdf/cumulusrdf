package edu.kit.aifb.cumulus.datasource.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import edu.kit.aifb.cumulus.datasource.serializer.Serializer;
import edu.kit.aifb.cumulus.framework.datasource.CounterDAO;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;

/**
 * {@link CounterDAO} implementation for Cassandra 2.x.
 * 
 * @author Sebastian Schmidt
 * @since 1.1.0
 * @param <K> The key class.
 */
public class Cassandra2xCounterDAO<K> implements CounterDAO<K> {

	private final CumulusDataAccessLayerFactory _factory;
	private final String _name;
	private final Session _session;
	private final Serializer<K> _serializer;
	private Long _defaultValue;

	private PreparedStatement _deleteCounterStatement;
	private PreparedStatement _incrementCounterStatement;
	private PreparedStatement _decrementCounterStatement;
	private PreparedStatement _getCounterStatement;
	
	/**
	 * Creates a new {@link Cassandra2xCounterDAO}.
	 * 
	 * @param factory the factory that created this counter.
	 * @param name the counter name, acting as an identifier.
	 * @param serializer the serializer used to serialize the name.
	 */
	Cassandra2xCounterDAO(
			final DataAccessLayerFactory factory,
			final String name,
			final Serializer<K> serializer) {
		_serializer = serializer;
		_factory = (CumulusDataAccessLayerFactory) factory;
		_name = name;
		_session = _factory.getSession();
	}

	@Override
	public void decrement(final K key, final Long delta) {
		_session.execute(_decrementCounterStatement.bind(delta, _serializer.serialize(key)));
	}

	@Override
	public void increment(final K key, final Long delta) {
		_session.execute(_incrementCounterStatement.bind(delta, _serializer.serialize(key)));
	}

	@Override
	public boolean contains(final K key) throws DataAccessLayerException {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void delete(final K... keys) throws DataAccessLayerException {
		for (final K key : keys) {
			_session.execute(_deleteCounterStatement.bind(_serializer.serialize(key)));
		}
	}

	@Override
	public Long get(final K key) throws DataAccessLayerException {
		final Row resultRow = _session.execute(_getCounterStatement.bind(_serializer.serialize(key))).one();

		if (resultRow != null) {
			return resultRow.getLong(0);
		} else {
			return _defaultValue;
		}
	}

	@Override
	public K getKey(final Long value) throws DataAccessLayerException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<K> keyIterator() throws DataAccessLayerException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<K> keySet() throws DataAccessLayerException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void set(final K key, final Long value) throws DataAccessLayerException {
		// Direct setting of counters not supported by CQL 3...
		Long currentValue = get(key);
		Long difference;

		if (currentValue == -1) {
			difference = 0L;
		} else {
			difference = value - currentValue;
		}

		_session.execute(_incrementCounterStatement.bind(difference, _serializer.serialize(key)));
	}

	@Override
	public void setAll(final Map<K, Long> pairs) throws DataAccessLayerException {
		for (Map.Entry<K, Long> pair : pairs.entrySet()) {
			set(pair.getKey(), pair.getValue());
		}
	}

	@Override
	public void setDefaultValue(final Long defaultValue) throws DataAccessLayerException {
		_defaultValue = defaultValue;
	}

	@Override
	public void createRequiredSchemaEntities() throws DataAccessLayerException {
		_session.execute("CREATE TABLE IF NOT EXISTS " + _name + " (key BLOB, value COUNTER, PRIMARY KEY (key))");

		_deleteCounterStatement = _session.prepare("DELETE FROM " + _name + " WHERE key = ?");
		_decrementCounterStatement = _session.prepare("UPDATE " + _name + " SET value = value - ? WHERE key = ?");
		_incrementCounterStatement = _session.prepare("UPDATE " + _name + " SET value = value + ? WHERE key = ?");
		_getCounterStatement = _session.prepare("SELECT value FROM " + _name + " WHERE key = ?");		
	}
}
