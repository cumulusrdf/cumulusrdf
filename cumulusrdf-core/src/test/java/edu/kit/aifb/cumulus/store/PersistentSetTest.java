package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.TestUtils.newTripleStore;
import static edu.kit.aifb.cumulus.TestUtils.randomString;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.kit.aifb.cumulus.AbstractCumulusTest;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;

public class PersistentSetTest extends AbstractCumulusTest {

	private static PersistentSet<String> CLASS_UNDER_TEST;
	private static final Set<String> TESTDATA1, TESTDATA2;

	static {

		TESTDATA1 = new HashSet<String>();
		TESTDATA1.add("1");
		TESTDATA1.add("2");
		TESTDATA1.add("3");
		TESTDATA1.add("4");

		TESTDATA2 = new HashSet<String>();
		TESTDATA2.add("8");
		TESTDATA2.add("20");
		TESTDATA2.add("-3");
		TESTDATA2.add("5");

	}


	@BeforeClass
	public static void beforeAllTests() throws Exception {
		
		_tripleStore = newTripleStore();
		_tripleStore.open();

		CLASS_UNDER_TEST = new PersistentSet<String>(String.class, "SET_" + randomString());
		CLASS_UNDER_TEST.initialise(_tripleStore.getDataAccessLayerFactory());
	}

	@Test
	public void addAll() throws DataAccessLayerException {

		CLASS_UNDER_TEST.clear();
		assertTrue(CLASS_UNDER_TEST.isEmpty());

		assertTrue(CLASS_UNDER_TEST.addAll(TESTDATA2));
		assertTrue(CLASS_UNDER_TEST.size() == TESTDATA1.size());
	}

	@Test
	public void addTest() throws DataAccessLayerException {

		CLASS_UNDER_TEST.clear();
		assertTrue(CLASS_UNDER_TEST.isEmpty());

		for (String key : TESTDATA1) {
			assertTrue(CLASS_UNDER_TEST.add(key));
		}

		assertTrue(CLASS_UNDER_TEST.size() == TESTDATA1.size());
		assertTrue(CLASS_UNDER_TEST.containsAll(TESTDATA1));
	}

	@Test
	public void clearTest() throws DataAccessLayerException {
		CLASS_UNDER_TEST.clear();
		assertTrue(CLASS_UNDER_TEST.isEmpty());
	}

	@Test
	public void containsTest() throws DataAccessLayerException {

		for (String key : TESTDATA1) {
			assertTrue(CLASS_UNDER_TEST.contains(key));
		}

		assertTrue(CLASS_UNDER_TEST.containsAll(TESTDATA1));

		for (String key : TESTDATA2) {
			assertTrue(!CLASS_UNDER_TEST.contains(key));
		}

		assertTrue(!CLASS_UNDER_TEST.containsAll(TESTDATA2));
	}

	@Test
	public void iteratorTest() throws DataAccessLayerException {

		int count = 0;

		for (Iterator<String> iter = CLASS_UNDER_TEST.iterator(); iter.hasNext(); iter.next()) {
			count++;
		}

		assertTrue(count == TESTDATA1.size());
		assertTrue(count == CLASS_UNDER_TEST.size());
	}

	@Test
	public void removeTest() throws DataAccessLayerException {

		for (String key : TESTDATA2) {
			assertTrue(!CLASS_UNDER_TEST.remove(key));
		}

		assertTrue(CLASS_UNDER_TEST.size() == TESTDATA1.size());

		for (String key : TESTDATA1) {
			assertTrue(CLASS_UNDER_TEST.remove(key));
		}

		assertTrue(CLASS_UNDER_TEST.isEmpty());
	}

	@Before
	public void reset() throws DataAccessLayerException {

		CLASS_UNDER_TEST.clear();

		for (Iterator<String> iter = TESTDATA1.iterator(); iter.hasNext();) {
			CLASS_UNDER_TEST.add(iter.next());
		}
	}

	@Test
	public void retainTest() throws DataAccessLayerException {

		CLASS_UNDER_TEST.retainAll(TESTDATA1);
		assertTrue(CLASS_UNDER_TEST.size() == TESTDATA1.size());

		CLASS_UNDER_TEST.retainAll(TESTDATA2);
		assertTrue(CLASS_UNDER_TEST.isEmpty());
	}

	@Test
	public void toArrayTest() throws DataAccessLayerException {
		assertTrue(Arrays.equals(TESTDATA1.toArray(new String[0]), CLASS_UNDER_TEST.toArray(new String[0])));
	}
}