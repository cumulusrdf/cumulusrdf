package edu.kit.aifb.cumulus.store.dict.impl.string;

import java.util.Arrays;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.BIndex;

/**
 * Supertype layer for String dictionaries that are backed by a single {@link Index}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public abstract class SingleIndexStringDictionary extends StringDictionaryBase {
	protected BIndex _index;
	protected final String _indexName;
	
	/**
	 * Builds a new dictionary with a given index name.
	 * 
	 * @param id the dictionary identifier.
	 * @param indexName the index name.
	 */
	protected SingleIndexStringDictionary(final String id, final String indexName) {
		super(id);
		_indexName = indexName;
	}

	@Override
	protected final void initialiseInternal(final DataAccessLayerFactory factory) throws InitialisationException {
		_index = createIndex();
		_index.initialise(factory);
	}
	
	@Override
	protected void closeInternal() {
		// Nothing to be done here...
	}
	
	/**
	 * Creates a new identifier for a given string.
	 * The method takes care about (eventual) hash collision.
	 * 
	 * @param value the string value. 
	 * @param index the dictionary index that could already hold that resource / id.
	 * @return a new identifier for the given resource.
	 */
	protected byte[] newId(final String value, final BIndex index) {
		return IDMaker.nextID();
	}

	/**
	 * Creates and initializes the underlying index.
	 * 
	 * @return the underlying index.
	 */
	protected BIndex createIndex() {
		return new BIndex(_indexName);
	}
	
	/**
	 * Returns the N3 representation of the value associated with a given identifier.
	 * 
	 * @param id the value identifier.
	 * @param p the predicate flag.
	 * @return the N3 representation of the value associated with a given identifier.
	 * @throws DataAccessLayerException in case of data access failure. 
	 */
	protected String getStringValue(final byte[] id, final boolean p) throws DataAccessLayerException {

		final String value = _index.getQuick(id);
		if (value == null || value.isEmpty()) {
			_log.error(MessageCatalog._00086_NODE_NOT_FOUND_IN_DICTIONARY, Arrays.toString(id));
		}

		return value;
	}	

	/**
	 * Returns the underlying index name.
	 * 
	 * @return the underlying index name.
	 */
	public String getIndexName() {
		return _indexName;
	}
	
	@Override
	public void removeValue(final String value, final boolean p) throws DataAccessLayerException {
		_index.remove(value);
	}	
}