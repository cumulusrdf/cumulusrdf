package edu.kit.aifb.cumulus.store.dict.impl.string;

import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;

/**
 * A persistent string dictionary.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class PersistentStringDictionary extends SingleIndexStringDictionary {

	static final int ID_LENGTH = 8;
	static final byte[] EMPTY_VALUE = new byte[ID_LENGTH];

	/**
	 * Builds a new known dictionary for namespaces.
	 * 
	 * @param id the dictionary identifier.
	 * @param indexName the name of the underlying index.
	 */
	public PersistentStringDictionary(final String id, final String indexName) {
		super(id, indexName);
	}
	
	@Override
	protected byte[] getIdInternal(final String value, final boolean p) throws DataAccessLayerException {
		if (value.trim().length() == 0) {
			return EMPTY_VALUE;
		}
		
		byte[] id = null;

		synchronized (this) {
			id = _index.get(value);
			if (id[0] == NOT_SET[0]) {
				id = newId(value, _index);
				_index.putQuick(value, id);
			}
		}
		RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
		return id;
	}

	@Override
	protected String getValueInternal(final byte[] id, final boolean p) throws DataAccessLayerException {
		RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
		return _index.getQuick(id);
	}
}