package edu.kit.aifb.cumulus.store.dict.impl.value;

import static edu.kit.aifb.cumulus.TestUtils.T_DATA_ACCESS_LAYER_FACTORY;
import static edu.kit.aifb.cumulus.TestUtils.VALUE_FACTORY;
import static edu.kit.aifb.cumulus.TestUtils.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.Date;

import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.framework.util.Bytes;

/**
 * Test case for modes available in {@link CacheValueDictionary}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class CacheModeTest {

	private CacheValueDictionary _frontendDictionary;
	private CacheValueDictionary _firstLevelCache2;

	private ITopLevelDictionary _firstLevelDecoratee1;
	private ITopLevelDictionary _firstLevelDecoratee2;

	private ITopLevelDictionary _leafDictionary;

	byte[] _id = { 4, 5, 3, 2, 6, 3, 2, 1 };
	
	final Value _aValue = VALUE_FACTORY.createLiteral(new Date());

	/**
	 * First level cache test.
	 * 
	 * @throws Exception never, otherwise the corresponding test will fail.
	 */
	@Test
	public void firstLevelCache() throws Exception {
		
		_leafDictionary = mock(ITopLevelDictionary.class);
		_firstLevelDecoratee2 = new TransientValueDictionary(randomString(), _leafDictionary, 0);
		_firstLevelCache2 = new CacheValueDictionary(
				randomString(), 
				_firstLevelDecoratee2, 
				CacheValueDictionary.DEFAULT_CACHE_SIZE, 
				CacheValueDictionary.DEFAULT_CACHE_SIZE, 
				false);

		_firstLevelDecoratee1 = new TransientValueDictionary(randomString(), _firstLevelCache2, TransientValueDictionary.DEFAULT_THRESHOLD);
		_frontendDictionary = new CacheValueDictionary(
				randomString(), 
				_firstLevelDecoratee1, 
				CacheValueDictionary.DEFAULT_CACHE_SIZE, 
				CacheValueDictionary.DEFAULT_CACHE_SIZE, 
				true);

		_frontendDictionary.initialise(T_DATA_ACCESS_LAYER_FACTORY);
		
		final URI uri = VALUE_FACTORY.createURI("http://example.org#it");
		final byte [] id = _frontendDictionary.getID(uri, false);
		
		assertTrue(_frontendDictionary._node2id_cache.containsKey(uri));
		
		Value value = _frontendDictionary.getValue(id, false);
		assertEquals(uri, value);
		assertTrue(_frontendDictionary._id2node_cache.containsKey(ByteBuffer.wrap(id)));
		
		final StringBuilder builder = new StringBuilder();
		for (int i = 0; i < ((TransientValueDictionary)_firstLevelDecoratee1)._threshold + 1; i++) {
			builder.append('a');
		}

		final Literal longLiteral = VALUE_FACTORY.createLiteral(builder.toString());
		final byte [] longLiteralId = _frontendDictionary.getID(longLiteral, false);
		
		assertFalse(_frontendDictionary._node2id_cache.containsKey(longLiteral));
		assertTrue(_firstLevelCache2._node2id_cache.containsKey(longLiteral));
		
		value = _frontendDictionary.getValue(longLiteralId, false);
		assertEquals(longLiteral, value);
		
		assertTrue(_firstLevelCache2._id2node_cache.containsKey(ByteBuffer.wrap(Bytes.subarray(longLiteralId, 1, longLiteralId.length - 1))));
		assertFalse(_frontendDictionary._id2node_cache.containsKey(ByteBuffer.wrap(longLiteralId)));		
	}
}