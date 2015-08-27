package edu.kit.aifb.cumulus.datasource.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import edu.kit.aifb.cumulus.datasource.serializer.Serializer;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.datasource.MapDAO;

/**
 * {@link MapDAO} implementation for Cassandra 2.x storage module.
 * 
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 *
 * @param <K> the key kind.
 * @param <V> the value kind.
 */
public class Cassandra2xMapDAO<K, V> implements MapDAO<K, V> {

	protected final CumulusDataAccessLayerFactory _factory;
	protected final boolean _isBidirectional;

	protected final MapDAO<K, V> _delegate;

	/**
	 * Builds a new data access object with the given data.
	 * 
	 * @param factory the data access layer factory associated with this DAO.
	 * @param isBidirectional a flag indicating if this DAO should work in "bidirectional" mode.
	 * @param name the map name (acting as an identifier).
	 * @param keySerializer the serializer that will be used for keys.
	 * @param valueSerializer the serializer that will be used for values.
	 * @param ttl the time to live that will be used in INSERT operations.
	 */
	Cassandra2xMapDAO(
			final DataAccessLayerFactory factory,
			final boolean isBidirectional,
			final String name,
			final Serializer<K> keySerializer,
			final Serializer<V> valueSerializer,
			final int ttl) {

		_factory = (CumulusDataAccessLayerFactory) factory;
		_isBidirectional = isBidirectional;
		_delegate = _isBidirectional
				? new Cassandra2xBidirectionalMapDAO<K, V>(_factory.getSession(), name, ttl, keySerializer, valueSerializer)
				: new Cassandra2xDefaultMapDAO<K, V>(_factory.getSession(), name, _isBidirectional, ttl, keySerializer, valueSerializer);
	}
	
	@Override
	public boolean contains(final K key) throws DataAccessLayerException {
		if (key == null) {
			return false;
		}

		return _delegate.contains(key);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void delete(final K... keys) throws DataAccessLayerException {
		_delegate.delete(keys);
	}

	@Override
	public V get(final K key) throws DataAccessLayerException {
		return _delegate.get(key);
	}

	@Override
	public K getKey(final V value) throws DataAccessLayerException {
		return _delegate.getKey(value);
	}

	@Override
	public Iterator<K> keyIterator() throws DataAccessLayerException {
		return _delegate.keyIterator();
	}

	@Override
	public Set<K> keySet() throws DataAccessLayerException {
		return _delegate.keySet();
	}

	@Override
	public void set(final K key, final V value) throws DataAccessLayerException {
		_delegate.set(key, value);
	}

	@Override
	public void setAll(final Map<K, V> pairs) throws DataAccessLayerException {
		_delegate.setAll(pairs);
	}

	@Override
	public void setDefaultValue(final V defaultValue) throws DataAccessLayerException {
		_delegate.setDefaultValue(defaultValue);
	}

	@Override
	public void createRequiredSchemaEntities() throws DataAccessLayerException {
		_delegate.createRequiredSchemaEntities();
	}
}