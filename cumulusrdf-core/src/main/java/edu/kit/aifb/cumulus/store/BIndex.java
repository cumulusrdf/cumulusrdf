package edu.kit.aifb.cumulus.store;

import edu.kit.aifb.cumulus.framework.Initialisable;
import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.store.dict.impl.value.ValueDictionaryBase;

/**
 * A bidirectional index that can be accessed both by key and value.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class BIndex implements Initialisable{

	PersistentMap<byte[], String> _byId;
	PersistentMap<String, byte[]> _byValue;

	final String _indexName;
	
	/**
	 * Builds a new index with the given data.
	 * 
	 * @param indexName the name associated with this index.
	 */
	public BIndex(final String indexName) {
		_indexName = indexName;
	}
	
	@Override
	public void initialise(final DataAccessLayerFactory factory) throws InitialisationException {
		_byValue = new PersistentMap<String, byte[]>(
				String.class, 
				byte[].class,
				_indexName,
				false,
				ValueDictionaryBase.NOT_SET);
		_byValue.initialise(factory);

		_byId = new PersistentMap<byte[], String>(
				byte[].class,
				String.class,
				_indexName + "_REVERSE",
				false,
				"");
		_byId.initialise(factory);
	}
	
	/**
	 * Returns the id associated with the given value.
	 * 
	 * @param value the n3 value.
	 * @return the id associated with the given value.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public byte[] get(final String value) throws DataAccessLayerException {
		return _byValue.get(value);
	}

	/**
	 * Returns the value associated with the given id.
	 * 
	 * @param id the Cumulus internal id.
	 * @return the value associated with the given id.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public String getQuick(final byte[] id) throws DataAccessLayerException {
		return _byId.getQuick(id);
	}

	/**
	 * Puts the given pair on this index.
	 * 
	 * @param value the resource.
	 * @param id the id associated with that resource.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public void putQuick(final String value, final byte[] id) throws DataAccessLayerException {
		_byValue.putQuick(value, id);
		_byId.putQuick(id, value);
	}

	/**
	 * Returns true if this index contains the given id.
	 * 
	 * @param id the id of a given resource.
	 * @return true if this index contains the given id.
	 * @throws DataAccessLayerException in case of data access failure.

	 */
	public boolean contains(final byte[] id) throws DataAccessLayerException {
		return _byId.containsKey(id);
	}

	/**
	 * Removes the given resource from this index.
	 * 
	 * @param n3 the n3 representation of the resource to be removed.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public void remove(final String n3) throws DataAccessLayerException {
		_byId.removeQuick(_byValue.get(n3));
		_byValue.removeQuick(n3);
	}
}