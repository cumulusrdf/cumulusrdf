package edu.kit.aifb.cumulus.store.dict.impl.string;

import static edu.kit.aifb.cumulus.TestUtils.T_DATA_ACCESS_LAYER_FACTORY;
import static edu.kit.aifb.cumulus.TestUtils.randomString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import edu.kit.aifb.cumulus.framework.domain.dictionary.IDictionary;

/**
 * Test case for {@link CacheStringDictionary}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class CacheStringDictionaryTest {

	private CacheStringDictionary _cut;
	private IDictionary<String> _decoratee;

	byte[] _id = { 4, 5, 3, 2, 6, 3, 2, 1 };
	final String _aValue = String.valueOf(new Date());

	/**
	 * Setup fixture for this test.
	 * 
	 * @throws Exception never, otherwise the corresponding test will fail.
	 */ 
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		_decoratee = mock(IDictionary.class);
		_cut = new CacheStringDictionary(
				randomString(), 
				_decoratee, 
				CacheStringDictionary.DEFAULT_CACHE_SIZE, 
				CacheStringDictionary.DEFAULT_CACHE_SIZE, 
				false);

		_cut.initialise(T_DATA_ACCESS_LAYER_FACTORY);

		verify(_decoratee).initialise(T_DATA_ACCESS_LAYER_FACTORY);
	}

	/**
	 * In case the given cache size is lesser or equal to 0, then default value apply.
	 */
	@Test
	public void defaultCacheSize() {
		assertEquals(CacheStringDictionary.DEFAULT_CACHE_SIZE, _cut.cacheSize(-0));
		assertEquals(CacheStringDictionary.DEFAULT_CACHE_SIZE, _cut.cacheSize(-12));
		assertEquals(12345, _cut.cacheSize(12345));
	}
	
	/**
	 * In case the decoratee is null then an exception must be thrown.
	 */
	@Test
	public void decorateeIsNull() {
		try {
			_cut = new CacheStringDictionary(
					randomString(), 
					null, 
					CacheStringDictionary.DEFAULT_CACHE_SIZE, 
					CacheStringDictionary.DEFAULT_CACHE_SIZE, 
					false);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing, this is the expected behaviour
		}
	}

	/**
	 * Closing the dictionary means clearing the cache and invoking close() on decoratee.
	 */
	@Test
	public void close() {
		_cut.close();
		
		assertEquals(0, _cut._id2node_cache.size());
		assertEquals(0, _cut._node2id_cache.size());
		verify(_decoratee).close();
	}

	/**
	 * Tests ID creation and caching.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getID() throws Exception {

		when(_decoratee.getID(_aValue, false)).thenReturn(_id);

		assertTrue(_cut._node2id_cache.isEmpty());
		assertArrayEquals(_id, _cut.getID(_aValue, false));
		assertEquals(_id, _cut._node2id_cache.get(_aValue));
		assertArrayEquals(_id, _cut.getID(_aValue, false));
		
		verify(_decoratee).getID(_aValue, false);
	}

	/**
	 * Tests ID creation and caching.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValue() throws Exception {

		when(_decoratee.getValue(_id, false)).thenReturn(_aValue);

		assertTrue(_cut._id2node_cache.isEmpty());
		assertEquals(_aValue, _cut.getValue(_id, false));
		assertEquals(_aValue, _cut._id2node_cache.get(ByteBuffer.wrap(_id)));

		verify(_decoratee).getValue(_id, false);
	}

	/**
	 * If the input identifier is null the null must be returned.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValueWithNullId() throws Exception {
		assertNull(_cut.getValue(null, false));
		assertNull(_cut.getValue(null, true));
	}

	/**
	 * If the input value is null the null must be returned.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIdWithNullValue() throws Exception {
		assertNull(_cut.getID(null, false));
		assertNull(_cut.getID(null, true));
	}
}