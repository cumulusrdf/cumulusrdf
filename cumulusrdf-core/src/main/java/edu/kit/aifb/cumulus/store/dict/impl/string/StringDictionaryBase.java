package edu.kit.aifb.cumulus.store.dict.impl.string;

import edu.kit.aifb.cumulus.framework.domain.dictionary.DictionaryBase;
import edu.kit.aifb.cumulus.framework.domain.dictionary.DictionaryRuntimeContext;

/**
 * Supertype layer for all string dictionaries.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public abstract class StringDictionaryBase extends DictionaryBase<String> {

	protected static final ThreadLocal<DictionaryRuntimeContext> RUNTIME_CONTEXTS = new ThreadLocal<DictionaryRuntimeContext>() {
		protected DictionaryRuntimeContext initialValue() {
			return new DictionaryRuntimeContext();
		};
	};
	
	/**
	 * Builds a new dictionary with the given identifier.
	 * 
	 * @param id the dictionary identifier.
	 */
	public StringDictionaryBase(final String id) {
		super(id);
	}	
}