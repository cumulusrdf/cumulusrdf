package edu.kit.aifb.cumulus.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.AbstractIterator;

import edu.kit.aifb.cumulus.framework.Initialisable;
import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.datasource.MapDAO;

/**
 * A map implementations that read and write key/value pairs from a persistent
 * storage.
 * 
 * @author Andreas Wagner
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 * 
 * @param <K> the key kind / type managed by the underlying map structure.
 * @param <V> the value kind / type managed by the underlying map structure.
 */
public class PersistentMap<K, V> implements Initialisable {

	private final class Entry implements Map.Entry<K, V> {

		private final K _k;
		private V _v;

		/**
		 * Builds a new entry.
		 * 
		 * @param k the entry key.
		 * @param v the entry value.
		 */
		Entry(final K k, final V v) {
			_k = k;
			_v = v;
		}

		@Override
		public boolean equals(final Object obj) {

			if (obj == this) {
				return true;
			}

			@SuppressWarnings("unchecked")
			final Map.Entry<K, V> e = (Map.Entry<K, V>) obj;
			final Object k1 = getKey();
			final Object k2 = e.getKey();

			if (k1 == k2 || (k1 != null && k1.equals(k2))) {

				final Object v1 = getValue();
				final Object v2 = e.getValue();

				if (v1 == v2 || (v1 != null && v1.equals(v2))) {
					return true;
				}
			}

			return false;
		}

		@Override
		public K getKey() {
			return _k;
		}

		@Override
		public V getValue() {
			return _v;
		}

		@Override
		public int hashCode() {
			return (_k == null ? 0 : _k.hashCode()) ^ (_v == null ? 0 : _v.hashCode());
		}

		@Override
		public V setValue(final V value) {
			this._v = value;
			return _v;
		}

		@Override
		public String toString() {
			return "[key = " + _k + ", value = " + _v + "]";
		}
	}

	private Class<K> _k;
	private Class<V> _v;

	private MapDAO<K, V> _dao;

	private int _size;
	private final boolean _isBidirectional;
	private final String _name;
	private final V _defaultValue;

	/**
	 * Builds a new persistent map with a given name.
	 * 
	 * @param name the map name.
	 * @param k the key class.
	 * @param v the value class.
	 * @param bidirectional if this map is bidirectional.
	 * @param defaultValue the default value that will be returned in case of
	 *            empty search result.
	 */
	public PersistentMap(final Class<K> k, final Class<V> v, final String name, final boolean bidirectional,
			final V defaultValue) {
		_k = k;
		_v = v;
		_name = name;
		_isBidirectional = bidirectional;
		this._defaultValue = defaultValue;
	}

	/**
	 * Clears all entries from the underlying persistent entity.
	 * 
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public void clear() throws DataAccessLayerException {

		for (final K key : keySet()) {
			remove(key);
		}
		_size = 0;
	}

	/**
	 * Returns true if this map contains the given key.
	 * 
	 * @param key the key.
	 * @return true if this map contains the given key, false otherwise.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public boolean containsKey(final K key) throws DataAccessLayerException {
		return (key != null && _dao.contains(key));
	}

	/**
	 * Returns true if this map contains the given value.
	 * 
	 * @param value the value.
	 * @return true if this map contains the given value, false otherwise.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public boolean containsValue(final V value) throws DataAccessLayerException {

		if (value == null) {
			return false;
		}

		if (_isBidirectional) {
			return getKeyQuick(value) != null;
		} else {
			for (final K key : _dao.keySet()) {
				if (_dao.get(key).equals(value)) {
					return true;
				}
			}

			return false;
		}
	}

	/**
	 * Returns a set containing all keys within thid map.
	 * 
	 * @return a set containing all keys within thid map.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public Set<Map.Entry<K, V>> entrySet() throws DataAccessLayerException {

		final Set<Map.Entry<K, V>> entrySet = new HashSet<Map.Entry<K, V>>();

		for (final K key : keySet()) {
			entrySet.add(new Entry(key, get(key)));
		}

		return entrySet;
	}

	/**
	 * Returns the value associated with the given key.
	 * 
	 * @param key the key.
	 * @return the value associated with the given key.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public V get(final K key) throws DataAccessLayerException {

		if (key == null) {
			return null;
		}

		return _dao.get(key);
	}

	/**
	 * Returns the key associated with the given value.
	 * 
	 * @param value the value.
	 * @return the key associated with the given value.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public K getKeyQuick(final V value) throws DataAccessLayerException {
		return _dao.getKey(value);
	}

	/**
	 * Returns the value associated with the given key.
	 * 
	 * @param key the key.
	 * @return the value associated with the given key.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public V getQuick(final K key) throws DataAccessLayerException {
		return _dao.get(key);
	}

	@Override
	public void initialise(final DataAccessLayerFactory factory) throws InitialisationException {
		_dao = factory.getMapDAO(_k, _v, _isBidirectional, _name);
		try {
			_dao.setDefaultValue(_defaultValue);
			_dao.createRequiredSchemaEntities();
		} catch (final DataAccessLayerException exception) {
			throw new InitialisationException(exception);
		}
	}

	/**
	 * Returns true if this map is empty.
	 * 
	 * @return true if this map is empty.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public boolean isEmpty() throws DataAccessLayerException {
		return size() == 0;
	}

	/**
	 * Returns an iterator over the keys of this map.
	 * 
	 * @return an iterator over the keys of this map.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public Iterator<K> keyIterator() throws DataAccessLayerException {
		return _dao.keyIterator();
	}

	/**
	 * Returns a set containing all the keys of this map.
	 * 
	 * @return a set containing all the keys of this map.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public Set<K> keySet() throws DataAccessLayerException {
		return _dao.keySet();
	}

	/**
	 * Puts the given entry key/value into this map. If a mapping already exists
	 * for that key, it will be replaced with the new value.
	 * 
	 * @param key the key.
	 * @param value the value.
	 * @return the old mapping (if exists).
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public V put(final K key, final V value) throws DataAccessLayerException {

		if (key == null || value == null) {
			return null;
		}

		final V old_value = get(key);
		_dao.set(key, value);

		if (old_value == null) {
			_size++;
		}

		return old_value;
	}

	/**
	 * Puts the given entries into this map. If a mapping already exists for a
	 * key, it will be replaced with the new value.
	 * 
	 * @param m the entries to insert.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public void putAll(final Map<? extends K, ? extends V> m) throws DataAccessLayerException {

		for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * Puts the given entry key/value into this map. If a mapping already exists
	 * for that key, it will be replaced with the new value.
	 * <p>
	 * In contrast to {@link #put}, this does not return the old value.
	 * 
	 * @param key the key.
	 * @param value the value.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public void putQuick(final K key, final V value) throws DataAccessLayerException {

		if (key == null || value == null) {
			return;
		}

		_dao.set(key, value);

		_size = -1;
	}

	/**
	 * Removes the entry with the given key from the map.
	 * 
	 * @param key the key.
	 * @return the old value mapped to the key, or null if nothing was mapped to
	 *         that key.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	@SuppressWarnings("unchecked")
	public V remove(final K key) throws DataAccessLayerException {

		if (key == null) {
			return null;
		}

		final V oldValue = get(key);

		if (oldValue == null) {
			return null;
		}

		_dao.delete(key);

		_size--;
		return oldValue;
	}

	/**
	 * Removes the entry with the given key from the map.
	 * 
	 * @param key the key.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	@SuppressWarnings("unchecked")
	public void removeQuick(final K key) throws DataAccessLayerException {

		if (key == null) {
			return;
		}

		_dao.delete((K) key);
		_size = -1;
	}

	/**
	 * Returns the amount of entries in this map.
	 * 
	 * @return the amount of entries in this map.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public int size() throws DataAccessLayerException {
		if (_size == -1) {
			_size = entrySet().size();
		}

		return _size;
	}

	/**
	 * Returns an iterator over the values of this map.
	 * 
	 * @return an iterator over the values of this map.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public Iterator<V> valueIterator() throws DataAccessLayerException {

		final Iterator<K> _key_iter = keyIterator();

		return new AbstractIterator<V>() {

			@Override
			public V computeNext() {

				try {
					if (!_key_iter.hasNext()) {
						return endOfData();
					}

					K key = _key_iter.next();

					if (key == null) {
						return endOfData();
					}

					return get(key);
				} catch (final DataAccessLayerException exception) {
					throw new RuntimeException(exception);
				}
			}
		};
	}

	/**
	 * Returns a collection containing all values of this map.
	 * 
	 * @return a collection containing all values of this map.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public Collection<V> values() throws DataAccessLayerException {

		final List<V> values = new LinkedList<V>();

		for (final K key : keySet()) {

			final Object val = get(key);

			if (val != null) {
				values.add(get(key));
			}
		}

		return values;
	}
}