package edu.kit.aifb.cumulus.store.dict.impl.string;

import static edu.kit.aifb.cumulus.framework.Environment.CHARSET_UTF8;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;

/**
 * Dictionary that uses strings encoding for generating variable-length identifiers.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class TransientStringDictionary extends StringDictionaryBase {

	static final int DEFAULT_THRESHOLD = 1000; // 1K
	
	static final byte THRESHOLD_EXCEEDED = 1;
	static final byte THRESHOLD_NOT_EXCEEDED = 2;
	
	/**
	 * Builds a new dictionary.
	 * 
	 * @param id the dictionary identifier.
	 */
	public TransientStringDictionary(final String id) {
		super(id);
	}

	@Override
	protected void closeInternal() {
		// Nothing to be done here...
	}

	@Override
	protected String getValueInternal(final byte[] id, final boolean p) {
		RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
		return new String(id, CHARSET_UTF8);
	}

	@Override
	protected byte[] getIdInternal(final String value, final boolean p) {
		return value.getBytes(CHARSET_UTF8);
	}

	@Override
	public void removeValue(final String value, final boolean p) {
		// Nothing to be done here...
	}

	@Override
	public void initialiseInternal(final DataAccessLayerFactory factory) {
		// Nothing to be done here...
	}
}