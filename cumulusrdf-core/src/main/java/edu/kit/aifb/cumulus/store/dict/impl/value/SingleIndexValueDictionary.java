package edu.kit.aifb.cumulus.store.dict.impl.value;

import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.BIndex;

/**
 * Supertype layer for dictionaries that are backed by a single {@link Index}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public abstract class SingleIndexValueDictionary extends ValueDictionaryBase {
	protected BIndex _index;
	protected final String _indexName;

	/**
	 * Builds a new dictionary with a given index name.
	 * 
	 * @param id the dictionary identifier.
	 * @param indexName the index name.
	 */
	protected SingleIndexValueDictionary(final String id, final String indexName) {
		super(id);
		_indexName = indexName;
	}

	@Override
	protected void initialiseInternal(final DataAccessLayerFactory factory) throws InitialisationException {
		_index = createIndex();
		_index.initialise(factory);
	}
	
	/**
	 * Returns the identifier of a given N3 resource.
	 * 
	 * @param n3 the resource (N3 representation).
	 * @param p a flag indicating if the resource is a predicate.
	 * @return the identifier of the given resource.
	 * @throws DataAccessLayerException in case of data access failure. 
	 */
	protected byte[] getID(final String n3, final boolean p) throws DataAccessLayerException {
		return (n3 == null || n3.isEmpty() || n3.charAt(0) == '?')
				? null
				: _index.get(n3);
	}
	
	/**
	 * Resolves hash collision.
	 * 
	 * @param id the computed (hash) identifier.
	 * @param step the number of step to use in algorithm.
	 * @return the resolved hash identifier.
	 */
	protected byte[] resolveHashCollision(final byte[] id, final int step) {

		final ByteBuffer buffer = ByteBuffer.wrap(id);
		long hash = buffer.getLong(1);
		// linear probing
		buffer.putLong(1, ++hash).flip();
		return buffer.array();
	}
	
	/**
	 * Creates a new identifier for a given resource.
	 * The method takes care about (eventual) hash collision.
	 * 
	 * @param n3 the N3 representation of resource.
	 * @param index the dictionary index that could already hold that resource / id.
	 * @return a new identifier for the given resource.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	protected byte[] newId(final String n3, final BIndex index) throws DataAccessLayerException {
		byte[] id = makeNewHashID(n3);
		for (int i = 0; index.contains(id) && i <= 100; i++) {

			id = resolveHashCollision(id, i);

			if (i == 100) {
				_log.error(MessageCatalog._00071_UNABLE_TO_RESOLVE_COLLISION, n3, i);
			}
		}
		return id;
	}

	/**
	 * Makes a new hash identifier.
	 * 
	 * @param n3 the N3 representation of a given resource.
	 * @return the identifier associated with a given resource.
	 */
	protected abstract byte[] makeNewHashID(final String n3);

	/**
	 * Creates the underlying index.
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
	 * @throws DataAccessLayerException in case of data access layer factory. 
	 */
	protected String getN3(final byte[] id, final boolean p) throws DataAccessLayerException {

		final String n3 = _index.getQuick(id);
		if (n3 == null || n3.isEmpty()) {
			_log.error(MessageCatalog._00086_NODE_NOT_FOUND_IN_DICTIONARY, Arrays.toString(id));
		}

		return n3;
	}	
}