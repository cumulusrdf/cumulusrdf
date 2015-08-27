package edu.kit.aifb.cumulus.store.dict.impl.string;

import static org.junit.Assert.assertSame;

import org.junit.Test;

import edu.kit.aifb.cumulus.framework.domain.dictionary.DictionaryRuntimeContext;

/**
 * Test case for {@link StringDictionaryBase}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class StringDictionaryBaseTest {
	/**
	 * The dictionary must own a dictionary runtime context.
	 */
	@Test
	public void dictionaryRuntimeContext() {
		final DictionaryRuntimeContext context = StringDictionaryBase.RUNTIME_CONTEXTS.get();

		final DictionaryRuntimeContext mustBeTheSame = StringDictionaryBase.RUNTIME_CONTEXTS.get();

		assertSame(context, mustBeTheSame);
	}
}
