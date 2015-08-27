package edu.kit.aifb.cumulus.framework.datasource;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Data access object interface for persistent maps.
 * Note that *ALL* methods throws {@link DataAccessLayerException} because 
 * theoretically they could interact with a persistent storage.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 * 
 * @param <K> the key kind / type managed by the underlying map structure.
 * @param <V> the value kind / type managed by the underlying map structure.
 */
public interface MapDAO<K, V> {
	
	/**
	 * Returns true if the underlying map structure contains the given key.
	 * 
	 * @param key the key.
	 * @return true if the underlying map structure contains the given key.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	boolean contains(final K key) throws DataAccessLayerException;
	
	/**
	 * Deletes a set of keys from the underlying map structure.
	 * 
	 * @param keys the keys that will be removed.
	 * @throws DataAccessLayerException in case of data access failure.
	 */	
	@SuppressWarnings("unchecked")
	void delete(K... keys) throws DataAccessLayerException;
	
	/**
	 * Returns the value associated with a given key.
	 * In case of no value can be found, if a default value has been set then it's returned, 
	 * otherwise it returns null.
	 * 
	 * @param key the key (i.e. the value identity).
	 * @return the value associated with a given key, a default value or null if no value can be found.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	V get(K key) throws DataAccessLayerException;
	
	/**
	 * Returns the key associated with a given value.
	 * 
	 * @param value the value acting as search criteria.
	 * @return the key associated with that value.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	K getKey(V value) throws DataAccessLayerException;
	
	/**
	 * Returns an iterator over all keys managed by this data access object.
	 * 
	 * @return an iterator over all keys managed by this data access object.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	Iterator<K> keyIterator() throws DataAccessLayerException;
	
	/**
	 * Returns a set containing all keys managed by this data access object.
	 * 
	 * @return a set containing all keys managed by this data access object.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	Set<K> keySet() throws DataAccessLayerException;
	
	/**
	 * Persists a new key/value pair on the underlying storage.
	 * 
	 * @param key the key.
	 * @param value the value.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void set(final K key, final V value) throws DataAccessLayerException;

	/**
	 * Persists a set of given key/value pairs on the underlying storage.
	 * 
	 * @param pairs the key/value pairs to be persisted.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void setAll(Map<K, V> pairs) throws DataAccessLayerException;
	
	/**
	 * Injects the default value that will be used as result in case of empty search.
	 * 
	 * @param defaultValue the default value.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void setDefaultValue(V defaultValue) throws DataAccessLayerException;
	
	/**
	 * Initialises datasource-specific resources related with this data access object.
	 * 
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void createRequiredSchemaEntities() throws DataAccessLayerException;
}