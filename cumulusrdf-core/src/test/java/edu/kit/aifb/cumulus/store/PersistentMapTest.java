package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.TestUtils.newTripleStore;
import static edu.kit.aifb.cumulus.TestUtils.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.kit.aifb.cumulus.AbstractCumulusTest;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;

/**
 * Test case for {@link PersistentMap}.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class PersistentMapTest extends AbstractCumulusTest {

	private static final Map<String, String> TESTDATA1, TESTDATA2, TESTDATA3, TESTDATA4;
	private static PersistentMap<String, String> map;

	static {

		TESTDATA1 = new HashMap<String, String>();
		TESTDATA1.put("1", "1");
		TESTDATA1.put("2", "2");
		TESTDATA1.put("3", "3");
		TESTDATA1.put("4", "4");

		TESTDATA2 = new HashMap<String, String>();
		TESTDATA2.put("1", "2");
		TESTDATA2.put("2", "3");
		TESTDATA2.put("3", "4");
		TESTDATA2.put("4", "5");

		TESTDATA3 = new HashMap<String, String>();
		TESTDATA3.put("1", "2");
		TESTDATA3.put("2", "2");
		TESTDATA3.put("3", "3");
		TESTDATA3.put("4", "5");
		TESTDATA3.put("5", "5");
		TESTDATA3.put("6", "5");

		TESTDATA4 = new HashMap<String, String>();
		TESTDATA4.put("7", "7");
		TESTDATA4.put("8", "8");
		TESTDATA4.put("9", "9");
		TESTDATA4.put("10", "10");
	}

	@BeforeClass
	public static void beforeAllTests() throws Exception {
		
		_tripleStore = newTripleStore();
		_tripleStore.open();

		map = new PersistentMap<String, String>(String.class, String.class, "MAP_" + randomString(), false, null);
		map.initialise(_tripleStore.getDataAccessLayerFactory());
	}
	
	@Test
	public void clearTest() throws DataAccessLayerException {
		map.clear();
		assertTrue(map.isEmpty());
	}

	@Test
	public void containsKeyTest() throws DataAccessLayerException {

		for (String key : TESTDATA1.keySet()) {
			assertTrue(map.containsKey(key));
		}

		for (String key : TESTDATA4.keySet()) {
			assertTrue(!map.containsKey(key));
		}
	}

	@Test
	public void containsValueTest() throws DataAccessLayerException {

		for (String key : TESTDATA1.keySet()) {
			assertTrue(map.containsValue(TESTDATA1.get(key)));
		}

		for (String key : TESTDATA4.keySet()) {
			assertTrue(!map.containsValue(TESTDATA4.get(key)));
		}
	}

	@Test
	public void overwriteTest() throws DataAccessLayerException {

		map.clear();
		assertTrue(map.isEmpty());

		map.putAll(TESTDATA1);
		map.putAll(TESTDATA2);

		assertEquals(map.size(), TESTDATA2.size());
		assertTrue(map.entrySet().containsAll(TESTDATA2.entrySet()));
	}

	@Test
	public void duplicatesTest() throws DataAccessLayerException {

		map.clear();
		assertTrue(map.isEmpty());

		map.putAll(TESTDATA1);
		map.putAll(TESTDATA1);
		map.putAll(TESTDATA1);

		assertTrue(map.size() == TESTDATA1.size());
	}

	@Test
	public void entrySetTest() throws DataAccessLayerException {
		assertEquals(TESTDATA1.keySet(), map.keySet());
	}

	@Test
	public void getValueTest() throws DataAccessLayerException {

		for (String key : TESTDATA1.keySet()) {
			assertEquals(TESTDATA1.get(key), map.get(key));
		}
	}

	@Test
	public void insertAllTest() throws DataAccessLayerException {

		map.clear();
		assertTrue(map.isEmpty());

		map.putAll(TESTDATA1);

		for (String key : TESTDATA1.keySet()) {
			assertTrue(map.containsKey(key));
		}

		for (String key : TESTDATA1.keySet()) {
			assertEquals(TESTDATA1.get(key), map.get(key));
		}
	}

	@Test
	public void insertTest() throws DataAccessLayerException {

		for (String key : TESTDATA2.keySet()) {
			assertEquals(TESTDATA1.get(key), map.put(key, TESTDATA2.get(key)));
		}

		for (String key : TESTDATA2.keySet()) {
			assertEquals(TESTDATA2.get(key), map.get(key));
		}
	}

	@Test
	public void keySetTest() throws DataAccessLayerException {
		assertEquals(TESTDATA1.keySet(), map.keySet());
	}

	@Test
	public void removeTest() throws DataAccessLayerException {

		assertEquals(TESTDATA1.size(), map.size());

		for (String key : TESTDATA4.keySet()) {
			assertNull(map.remove(key));
		}

		for (String key : TESTDATA1.keySet()) {
			assertEquals(TESTDATA1.get(key), map.remove(key));
		}

		assertTrue(map.isEmpty());
	}

	@Before
	public void reset() throws DataAccessLayerException {

		for (String key : map.keySet()) {
			map.remove(key);
		}

		for (Map.Entry<String, String> entry : TESTDATA1.entrySet()) {
			map.put(entry.getKey(), entry.getValue());
		}
	}

	@Test
	public void valuesTest() throws DataAccessLayerException {
		assertTrue(TESTDATA1.values().containsAll(map.values()));
	}
}