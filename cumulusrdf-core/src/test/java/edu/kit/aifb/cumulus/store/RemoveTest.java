package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.TestUtils.SELECT_ALL_TRIPLES_PATTERN;
import static edu.kit.aifb.cumulus.TestUtils.buildLiteral;
import static edu.kit.aifb.cumulus.TestUtils.buildResource;
import static edu.kit.aifb.cumulus.TestUtils.clean;
import static edu.kit.aifb.cumulus.TestUtils.newTripleStore;
import static edu.kit.aifb.cumulus.TestUtils.numOfRes;
import static edu.kit.aifb.cumulus.util.Util.parseNX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFFormat;

import edu.kit.aifb.cumulus.AbstractCumulusTest;

/**
 * Test case for triple deletion.
 * 
 * @author Andreas Wagner
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class RemoveTest extends AbstractCumulusTest {

	private final int _howManyTriplesInDataset = 325;
	
	private Value[][] _patterns_triple = {
			new Value[] {buildResource("http://gridpedia.org/id/Driver"), null, null},
			new Value[] {buildResource("http://gridpedia.org/id/Owner"), null, null},
			new Value[] {buildResource("http://gridpedia.org/id/EnergyData"), null, null},
			new Value[] {buildResource("http://gridpedia.org/id/LoadProfile"), null, null},

			// ?x y1 ?z
			new Value[] { null, buildResource("http://www.w3.org/2000/01/rdf-schema#seeAlso"), null },
			new Value[] { null, buildResource("http://www.w3.org/2000/01/rdf-schema#subClassOf"), null },

			// ?x ?y z1
			new Value[] { null, null, buildLiteral("2012-02-01T09:53:00Z", buildResource("http://www.w3.org/2001/XMLSchema#dateTime"))},
			new Value[] { null, null, buildLiteral("2012-01-30T17:46:41Z", buildResource("http://www.w3.org/2001/XMLSchema#dateTime")) },
			new Value[] { null, null, buildResource("http://gridpedia.org/wiki/Media") },
			new Value[] { null, null, buildResource("http://dbpedia.org/resource/Building") },

			// ?x y1 z1
			new Value[] { null, OWL.SAMEAS, OWL.THING },
			new Value[] { null, OWL.SAMEAS, buildResource("http://dbpedia.org/resource/Data") },
			new Value[] { null, RDFS.LABEL, buildLiteral("Data") },

			// x1 y1 ?z
			new Value[] { buildResource("http://gridpedia.org/id/BatteryData"), RDFS.LABEL, null },
			new Value[] { buildResource("http://gridpedia.org/id/flow"), RDFS.ISDEFINEDBY, null },
			new Value[] { buildResource("http://gridpedia.org/id/locationLat"), RDFS.DOMAIN, null },

			// x1 ?y z1
			new Value[] { buildResource("http://gridpedia.org/id/Thing"), null, buildLiteral("Thing") },
			new Value[] { buildResource("http://gridpedia.org/id/SoftwareProgram"), null, RDFS.CLASS },

			// x1 y1 z1
			new Value[] { buildResource("http://gridpedia.org/id/Actor"), RDFS.LABEL, buildLiteral("Actor") },
			new Value[] { buildResource("http://gridpedia.org/id/Organization"), RDFS.LABEL, buildLiteral("Organization") },
			new Value[] { buildResource("http://gridpedia.org/id/SoftwareProgram"),
					buildResource("http://semantic-mediawiki.org/swivt/1.0#wikiPageModificationDate"),
					buildLiteral("2012-01-31T14:03:46Z", buildResource("http://www.w3.org/2001/XMLSchema#dateTime")) }
	};

	@SuppressWarnings("unused")
	private Value[][] _pattern_quads = {
			new Value[] { null, null, null, buildResource("http://rdf.opiumfield.com/lastfm/profile/pouryascarface") },
			new Value[] {
					null,
					null,
					null,
					buildResource("http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E") },
			new Value[] { null, null, null, buildResource("http://dbpedia.org/data/Dorthe_Andersen.xml") }			
	};

	private Value[][] _pattern_emtpy_query_result = {
			new Value[] { buildResource("http://gridpedia.org/id/Thing"), null, buildLiteral("Data") },
			new Value[] { null, RDFS.LABEL, buildResource("http://dbpedia.org/resource/Owner") },
			new Value[] { buildResource("http://gridpedia.org/id/EnergyConsumer2"), null, null }			
	};

	private Value[][] _triple_not_existing = {
			new Value[] { 
					buildResource("http://gridpedia.org/id/EnergyConsumer2"), 
					buildResource("http://www.w3.org/2000/01/rdf-schema#label"), 
					buildLiteral("Data") },
			new Value[] { 
					buildResource("http://gridpedia.org/id/Attacker"),
					buildResource("http://semantic-mediawiki.org/swivt/1.0#wikiPageModificationDate"),
					buildLiteral("2011-01-31T09:03:03Z", buildResource("http://www.w3.org/2001/XMLSchema#dateTime")) },
			new Value[] { 
					buildResource("http://gridpedia.org/id/Attacker"), 
					RDF.TYPE,
					buildResource("http://gridpedia.org/wiki/Provider") }
	};
	
	private Value[] _query_pattern_results = { buildResource("http://gridpedia.org/id/Actor"), null, null };
	private Value[] _query_pattern_no_results = { null, RDFS.LABEL, buildLiteral("Actor") };

	/**
	 * Setup fixture for the whole test case.
	 * 
	 * @throws Exception never, otherwise the tests fail.
	 */
	@BeforeClass
	public static void beforeAllTests() throws Exception {
		_tripleStore = newTripleStore();
		_tripleStore.open();
	}

	/**
	 * Clean up after each test.
	 * 
	 * @throws Exception never, otherwise the tests fail.
	 */
	@After
	public void afterEachTest() throws Exception {
		clean(_tripleStore);
	}

	/**
	 * Setup fixture before each test.
	 * 
	 * @throws Exception never, otherwise the tests fail.
	 */
	@Before
	public void beforeTest() throws Exception {
		_tripleStore.bulkLoad(DATA_NT, RDFFormat.NTRIPLES);
		assertEquals(_howManyTriplesInDataset, numOfRes(_tripleStore.query(SELECT_ALL_TRIPLES_PATTERN)));
	}

	/**
	 * Removes all data from the triple store.
	 * 
	 * @throws Exception never, otherwise the tests fail.
	 */
	@Test
	public void testRemoveAll() throws Exception {
		clean(_tripleStore);
		assertEquals(0, numOfRes(_tripleStore.query(SELECT_ALL_TRIPLES_PATTERN)));
	}

	/**
	 * If a pattern doesn't match any triple then no deletion happens.
	 * 
	 * @throws Exception never, otherwise the tests fail.
	 */
	@Test
	public void testRemoveEmptyQuery() throws Exception {
		for (final Value[] pattern : _pattern_emtpy_query_result) {

			assertEquals("query: " + Arrays.toString(pattern), 0, numOfRes(_tripleStore.query(pattern)));

			_tripleStore.removeData(pattern);

			assertEquals(_howManyTriplesInDataset, numOfRes(_tripleStore.query(SELECT_ALL_TRIPLES_PATTERN)));
		}

	}

	/**
	 * If a triple doesn't exist then no deletion happens.
	 * 
	 * @throws Exception never, otherwise the tests fail.
	 */
	@Test
	public void testRemoveNotExistingData() throws Exception {
		for (final Value[] triple : _triple_not_existing) {

			assertEquals("triple: " + Arrays.toString(triple), 0, numOfRes(_tripleStore.query(triple)));

			_tripleStore.removeData(triple);

			assertEquals(_howManyTriplesInDataset, numOfRes(_tripleStore.query(SELECT_ALL_TRIPLES_PATTERN)));
		}

	}

	/**
	 * Adds and then removes triples.
	 * 
	 * @throws Exception never, otherwise the tests fail.	 
	 */
	@Test
	public void testAddAndRemove() throws Exception {

		_tripleStore.addData(parseNX(
				"<http://izeus1.scc.kit.edu/id/i1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://gridpedia.org/id/TestType> . ")
				.iterator());
		_tripleStore.addData(parseNX(
				"<http://izeus1.scc.kit.edu/id/i2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://gridpedia.org/id/TestType> . ")
				.iterator());

		assertEquals(2, numOfRes(_tripleStore.query(new Value[] { null, RDF.TYPE, buildResource("http://gridpedia.org/id/TestType") })));

		_tripleStore.removeData(_tripleStore.query(new Value[] { buildResource("http://izeus1.scc.kit.edu/id/i1"), null, null }));

		assertEquals(1, numOfRes(_tripleStore.query(new Value[] { null, RDF.TYPE, buildResource("http://gridpedia.org/id/TestType") })));
	}

	/**
	 * If a valid triple pattern is removed then all matching triples will be removed.
	 * 
	 * @throws Exception never, otherwise the tests fail.	 
	 */
	@Test
	public void testRemoveValidTriplePatterns() throws Exception {

		for (final Value[] pattern : _patterns_triple) {

			assertTrue("query: " + Arrays.toString(pattern), numOfRes(_tripleStore.query(pattern)) > 0);

			_tripleStore.removeData(pattern);

			assertEquals("query: " + Arrays.toString(pattern), 0, numOfRes(_tripleStore.query(pattern)));
		}

		assertEquals(6, numOfRes(_tripleStore.query(_query_pattern_results)));
		assertEquals(0, numOfRes(_tripleStore.query(_query_pattern_no_results)));
	}
}