package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.TestUtils.SELECT_ALL_QUADS_PATTERN;
import static edu.kit.aifb.cumulus.TestUtils.buildResource;
import static edu.kit.aifb.cumulus.TestUtils.newQuadStore;
import static edu.kit.aifb.cumulus.TestUtils.newStatement;
import static edu.kit.aifb.cumulus.TestUtils.numOfRes;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;

import edu.kit.aifb.cumulus.AbstractCumulusTest;
import edu.kit.aifb.cumulus.TestUtils;

/**
 * Test deletion from the quad store.
 * @author Sebastian Schmidt
 * @since 1.0
 */
public class QuadsDeletionTest extends AbstractCumulusTest {

	private Statement[] _data = {
			newStatement("a1", "b1", "c1", "d1"),
			newStatement("a1", "b2", "c1", "d1"),			
			newStatement("a2", "b2", "c2", "d2"),
			newStatement("a3", "b3", "c3", "d3"),
			newStatement("a4", "b4", "c4", "d4"),	
			newStatement("a5", "b5", "c5", "d5"),	
			newStatement("a1", "b2", "c5", "d5"),
			newStatement("a3", "b3", "c4", "d1"),
			newStatement("a5", "b4", "c3", "d4"),
			newStatement("a2", "b5", "c2", "d2"),
			newStatement("a4", "b1", "c1", "d3")
	};
	
	/**
	 * Creates and opens the quad store.
	 * @throws Exception If the quad store could not be opened.
	 */
	@BeforeClass
	public static void setUp() throws Exception {
		_quadStore = newQuadStore();
		_quadStore.open();
	}

	/**
	 * Loads the test data into the quad store.
	 * @throws CumulusStoreException If the test data could not be loaded.
	 */
	@Before
	public void load() throws CumulusStoreException {
		_quadStore.addData(asList(_data).iterator());
		assertEquals(_data.length, numOfRes(_quadStore.query(SELECT_ALL_QUADS_PATTERN)));
	}

	/**
	 * Removes the test data from the quad store.
	 * @throws CumulusStoreException If the test data could not be removed.
	 */
	@After
	public void clear() throws CumulusStoreException {
		_quadStore.removeData(asList(_data).iterator());
		assertEmptyIterator(Arrays.toString(SELECT_ALL_QUADS_PATTERN), _quadStore.query(SELECT_ALL_QUADS_PATTERN));
	}

	/**
	 * Tests if the deletion of queries using the OC_PS index works.
	 * @throws CumulusStoreException If the deletion fails.
	 */
	@Test
	// OC_PS index
	public void testDeleteOCPS() throws CumulusStoreException {

		/*
		 * ?s ?p o c
		 */
		Value[] query = new Value[] {null, null, buildResource("c2"), buildResource("d2")};
		assertEquals(Arrays.toString(query), 2, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));

		/*
		 * ?s ?p o c
		 */
		query = new Value[] {null, null, buildResource("c1"), buildResource("d1")};
		assertEquals(Arrays.toString(query), 2, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));

		/*
		 * ?s ?p ?o c
		 */
		query = new Value[] {null, null, null, buildResource("d1")};
		assertEquals(Arrays.toString(query), 1, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));
	}

	/**
	 * Tests if deleting with queries with zero results really deletes nothing.
	 * @throws CumulusStoreException If the deletion fails.
	 */
	@Test
	// SC_OP index
	public void testDeleteInvalid() throws CumulusStoreException {

		final Value[] query = new Value[] { buildResource("d1"), null, null, buildResource("d1") };
		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));
	}

	/**
	 * Tests if the deletion of queries using the SC_OP index works.
	 * @throws CumulusStoreException If the deletion fails.
	 */
	@Test
	// SC_OP index
	public void testDeleteSCOP() throws CumulusStoreException {

		/*
		 * s ?p ?o c
		 */
		Value[] query = new Value[] {buildResource("a1"), null, null, buildResource("d1")};
		assertEquals(Arrays.toString(query), 2, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));

		query = new Value[] {buildResource("a1"), null, null, buildResource("d5")};
		assertEquals(Arrays.toString(query), 1, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));

		/*
		 * s ?p o c
		 */

		query = new Value[] {buildResource("a5"), null, buildResource("c3"), buildResource("d4")};
		assertEquals(Arrays.toString(query), 1, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));

		/*
		 * s p o c
		 */

		query = new Value[] {buildResource("a4"), buildResource("b1"), buildResource("c1"), buildResource("d3")};
		assertEquals(Arrays.toString(query), 1, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));
	}

	/**
	 * Tests if the deletion of queries using the triple indexes works.
	 * @throws CumulusStoreException If the deletion fails.
	 */
	@Test
	// different triple indexes
	public void testDeleteTripleIndexes() throws CumulusStoreException {

		/*
		 * s ?p ?o ?c
		 */
		Value[] query = new Value[] {buildResource("a5"), null, null, null};
		assertEquals(Arrays.toString(query), 2, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));

		query = new Value[] {buildResource("a1"), null, null, null};
		assertEquals(Arrays.toString(query), 3, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));

		TestUtils.numOfRes(_quadStore.query(SELECT_ALL_QUADS_PATTERN));

		/*
		 * ?s p ?o ?c
		 */
		query = new Value[] {null, buildResource("b2"), null, null};
		assertEquals(Arrays.toString(query), 1, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));

		/*
		 * ?s ?p o ?c
		 */
		query = new Value[] {null, null, buildResource("c1"), null};
		assertEquals(Arrays.toString(query), 1, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));
	}

	/**
	 * Tests if the deletion of queries using the SPC_O index works.
	 * @throws CumulusStoreException If the deletion fails.
	 */
	@Test
	// SPC_O index
	public void testDeleteSPCO() throws CumulusStoreException {

		/*
		 * s p ?o c
		 */
		Value[] query = new Value[] {buildResource("a1"), buildResource("b1"), null, buildResource("d1")};
		assertEquals(Arrays.toString(query), 1, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));

		query = new Value[] {buildResource("a5"), buildResource("b4"), null, buildResource("d4")};
		assertEquals(Arrays.toString(query), 1, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));

		/*
		 * s ?p ?o c
		 */
		query = new Value[] {buildResource("a1"), null, null, buildResource("d1")};
		assertEquals(Arrays.toString(query), 1, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));

		/*
		 * s p o c
		 */
		query = new Value[] {buildResource("a2"), buildResource("b5"), buildResource("c2"), buildResource("d2")};
		assertEquals(Arrays.toString(query), 1, TestUtils.numOfRes(_quadStore.query(query)));

		_quadStore.removeData(query);
		assertEmptyIterator(Arrays.toString(query), _quadStore.query(query));
	}
}