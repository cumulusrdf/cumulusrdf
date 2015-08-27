package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.TestUtils.randomString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

/**
 * Test case for {@link BIndex}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class BIndexTest {
	
	private BIndex _cut;
	private PersistentMap<byte[], String> _byId;
	private PersistentMap<String, byte[]> _byValue;
		
	/**
	 * Setup fixture for this test case.
	 */
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {
		final String indexName = randomString();
		_cut = new BIndex(indexName);
		
		_byId = mock(PersistentMap.class);
		_byValue = mock(PersistentMap.class);
		
		_cut._byId = _byId;
		_cut._byValue = _byValue;
	}
	
	/**
	 * get method should use the "ByValue" index.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void get() throws Exception {
		final String value = randomString();
		_cut.get(value);
		
		verify(_byValue).get(value);
	}
	
	/**
	 * getQuick method should use the "ById" index.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getQuick() throws Exception {
		final byte[] id = { 1, 2, 3, 4 };
		_cut.getQuick(id);
		
		verify(_byId).getQuick(id);
	}	
	
	/**
	 * putQuick method should use the both indexes.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void putQuick() throws Exception {
		final byte[] id = { 1, 2, 3, 4 };
		final String value = randomString();
		
		_cut.putQuick(value, id);
		
		verify(_byValue).putQuick(value, id);
		verify(_byId).putQuick(id, value);
	}		
	
	/**
	 * contains(id) method should use the "ById" index.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void contains() throws Exception {
		final byte[] id = { 1, 2, 3, 4 };
		_cut.contains(id);
		
		verify(_byId).containsKey(id);
	}	
	
	/**
	 * remove method should use the both indexes.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void remove() throws Exception {
		final byte[] id = { 1, 2, 3, 4 };
		final String value = randomString();
		
		when(_byValue.get(value)).thenReturn(id);
		
		_cut.remove(value);
		
		verify(_byValue).get(value);
		verify(_byId).removeQuick(id);		
		verify(_byValue).removeQuick(value);
	}			
}