package edu.kit.aifb.cumulus.store.dict.impl.value;

import static edu.kit.aifb.cumulus.TestUtils.T_DATA_ACCESS_LAYER_FACTORY;
import static edu.kit.aifb.cumulus.TestUtils.VALUE_FACTORY;
import static edu.kit.aifb.cumulus.TestUtils.randomString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Value;

import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;

/**
 * Test case for {@link CacheValueDictionary}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class CacheValueDictionaryTest {

	private CacheValueDictionary _cut;
	private ITopLevelDictionary _decoratee;

	byte[] _id = { 4, 5, 3, 2, 6, 3, 2, 1 };
	final Value _aValue = VALUE_FACTORY.createLiteral(new Date());

	/**
	 * Setup fixture for this test.
	 * 
	 * @throws Exception never, otherwise the corresponding test will fail.
	 */ 
	@Before
	public void setUp() throws Exception {

		_decoratee = mock(ITopLevelDictionary.class);
		_cut = new CacheValueDictionary(
				randomString(), 
				_decoratee, 
				CacheValueDictionary.DEFAULT_CACHE_SIZE, 
				CacheValueDictionary.DEFAULT_CACHE_SIZE, 
				false);

		_cut.initialise(T_DATA_ACCESS_LAYER_FACTORY);
		verify(_decoratee).initialise(T_DATA_ACCESS_LAYER_FACTORY);
	}

	/**
	 * In case the given cache size is lesser or equal to 0, then default value apply.
	 */
	@Test
	public void defaultCacheSize() {
		assertEquals(CacheValueDictionary.DEFAULT_CACHE_SIZE, _cut.cacheSize(-0));
		assertEquals(CacheValueDictionary.DEFAULT_CACHE_SIZE, _cut.cacheSize(-12));
		assertEquals(12345, _cut.cacheSize(12345));
	}
	
	/**
	 * In case the decoratee is null then an exception must be thrown.
	 */
	@Test
	public void decorateeIsNull() {
		try {
			_cut = new CacheValueDictionary(
					randomString(), 
					null, 
					CacheValueDictionary.DEFAULT_CACHE_SIZE, 
					CacheValueDictionary.DEFAULT_CACHE_SIZE, 
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
	 * compose() must invoke compose() on decoratee.
	 */
	@Test
	public void compose() {
		_cut.compose(_id, _id, _id);
		verify(_decoratee).compose(_id, _id, _id);

		_cut.compose(_id, _id);
		verify(_decoratee).compose(_id, _id);
	}

	/**
	 * decompose() must invoke compose() on decoratee.
	 */
	@Test
	public void decompose() {
		_cut.decompose(_id);
		verify(_decoratee).decompose(_id);
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

	/**
	 * The remove() method with a null argument has no effect.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeValueWithNullValue() throws Exception {
		assertTrue(_cut._node2id_cache.isEmpty());

		_cut.removeValue(null, false);

		assertTrue(_cut._node2id_cache.isEmpty());
		verifyNoMoreInteractions(_decoratee);
	}

	/**
	 * The remove() method must remove the value from cache and call remove() on decoratee.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeValue() throws Exception {
		when(_decoratee.getID(_aValue, false)).thenReturn(_id);

		assertTrue(_cut._node2id_cache.isEmpty());
		assertArrayEquals(_id, _cut.getID(_aValue, false));

		assertEquals(_id, _cut._node2id_cache.get(_aValue));
		assertArrayEquals(_id, _cut.getID(_aValue, false));

		_cut.removeValue(_aValue, false);

		assertTrue(_cut._node2id_cache.isEmpty());
		assertTrue(_cut._id2node_cache.isEmpty());
		verify(_decoratee).removeValue(_aValue, false);
	}

	/**
	 * The isBNode request must be forwarded to the decoratee. 
	 */
	@Test
	public void isBNode() {
		_cut.isBNode(_id);
		verify(_decoratee).isBNode(_id);
	}

	/**
	 * The isLiteral request must be forwarded to the decoratee. 
	 */
	@Test
	public void isLiteral() {
		_cut.isLiteral(_id);
		verify(_decoratee).isLiteral(_id);
	}

	/**
	 * The isResource request must be forwarded to the decoratee. 
	 */
	@Test
	public void isResource() {
		_cut.isResource(_id);
		verify(_decoratee).isResource(_id);
	}
}
