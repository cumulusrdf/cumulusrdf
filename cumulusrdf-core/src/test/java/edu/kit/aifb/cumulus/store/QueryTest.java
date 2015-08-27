package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.TestUtils.buildLiteral;
import static edu.kit.aifb.cumulus.TestUtils.buildResource;
import static edu.kit.aifb.cumulus.TestUtils.clean;
import static edu.kit.aifb.cumulus.TestUtils.newTripleStore;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFFormat;

import edu.kit.aifb.cumulus.AbstractCumulusTest;
import edu.kit.aifb.cumulus.TestUtils;

public class QueryTest extends AbstractCumulusTest {

	private static int[] limits = new int[] { 1, 2, 10, Integer.MAX_VALUE };

	private static Map<Value[], Integer> queries2count;
	private static List<Value[]> queriesSp, queriesOs, queriesPo;
	private static List<Value> queriesDescribe;
	private static Value[] queryAll;

	static {

		Value actor = buildResource("http://gridpedia.org/id/Actor"), individual = buildResource("http://gridpedia.org/id/Individual"), machine = buildResource(
				"http://gridpedia.org/id/Machine"), swift_page = buildResource("http://semantic-mediawiki.org/swivt/1.0#page");

		queriesSp = new LinkedList<Value[]>();
		queriesOs = new LinkedList<Value[]>();
		queriesPo = new LinkedList<Value[]>();

		// CF_S_PO
		queriesSp.add(new Value[] { actor, null, null });
		queriesSp.add(new Value[] { actor, RDFS.LABEL, null });
		queriesSp.add(new Value[] { actor, RDF.TYPE, null });
		queriesSp.add(new Value[] { null, RDF.TYPE, null });
		queriesSp.add(new Value[] { actor, RDF.TYPE, RDFS.CLASS });
		queriesSp.add(new Value[] { individual, null, null });
		queriesSp.add(new Value[] { individual, swift_page, null });
		queriesSp.add(new Value[] { machine, null, null });

		// CF_O_SP
		queriesOs.add(new Value[] { null, null, buildLiteral("Individual") });
		queriesOs.add(new Value[] { null, RDFS.LABEL, buildLiteral("Individual") });
		queriesOs.add(new Value[] { null, null, buildLiteral("Machine") });
		queriesOs.add(new Value[] { null, null, buildLiteral("2012-01-31T14:03:32Z", buildResource("http://www.w3.org/2001/XMLSchema#dateTime")) });
		queriesOs.add(new Value[] { null, null, actor });
		queriesOs.add(new Value[] { null, RDFS.SUBCLASSOF, actor });

		// CF_PO_S
		queriesPo.add(new Value[] { null, swift_page, null });
		queriesPo.add(new Value[] { null, RDFS.LABEL, null });
		queriesPo.add(new Value[] { null, OWL.SAMEAS, buildResource("http://dbpedia.org/resource/Device") });
		queriesPo.add(new Value[] { actor, null, buildResource("http://dbpedia.org/resource/Actant") });
		queriesPo.add(new Value[] { individual, null, buildResource("http://gridpedia.org/wiki/Individual") });

		queriesDescribe = Arrays.asList((Value) actor, (Value) individual, (Value) machine);

		queryAll = new Value[] { null, null, null };

		queries2count = new HashMap<Value[], Integer>();
		queries2count.put(new Value[] { null, null, null }, 325);
		queries2count.put(new Value[] { null, RDF.TYPE, null }, 42);
		queries2count.put(new Value[] { null, buildResource("http://semantic-mediawiki.org/swivt/1.0#page"), null }, 42);
		queries2count.put(new Value[] { null, buildResource("http://semantic-mediawiki.org/swivt/1.0#page2"), null }, 0);
		queries2count.put(new Value[] { null, RDFS.ISDEFINEDBY, null }, 42);
		queries2count.put(new Value[] { null, buildResource("http://semantic-mediawiki.org/swivt/1.0#wikiPageModificationDate"), null }, 42);
		queries2count.put(new Value[] { null, null, buildLiteral("2012-01-31T14:03:32Z", buildResource("http://www.w3.org/2001/XMLSchema#dateTime")) },
				1);
		queries2count.put(new Value[] { actor, null, null }, 10);
		queries2count.put(new Value[] { null, RDFS.LABEL, buildLiteral("Individual") }, 1);
		queries2count.put(new Value[] { actor, RDFS.SEEALSO, null }, 1);
		queries2count.put(new Value[] { actor, null, buildResource("http://dbpedia.org/resource/Actant") }, 1);
		queries2count.put(new Value[] { actor, null, buildResource("http://gridpedia.org/wiki/Individual") }, 0);
		queries2count.put(new Value[] { actor, RDF.TYPE, RDFS.CLASS }, 1);
		queries2count.put(new Value[] { machine, RDFS.LABEL, buildLiteral("Machine") }, 1);

	}

	@BeforeClass
	public static void setUp() throws Exception {
		_tripleStore = newTripleStore();
		_tripleStore.open();
	}
	
	@Before
	public void load() throws Exception {
		_tripleStore.bulkLoad(DATA_NT, RDFFormat.NTRIPLES);		
	}

	@After
	public void clear() throws CumulusStoreException {
		clean(_tripleStore);
	}

	@Test
	public void testDescribe() throws Exception {

		for (boolean hops : new boolean[] { false, true }) {

			for (int limit_s : new int[] { 1, 5, Integer.MAX_VALUE }) {

				for (int limit_o : new int[] { 1, 5, Integer.MAX_VALUE }) {

					for (final Value q : queriesDescribe) {

						final Iterator<Statement> it = _tripleStore.describe(q, hops, limit_s, limit_o);

						assertNotNull(it);
						int numOfRes = TestUtils.numOfRes(it);
						assertTrue(numOfRes >= 1);

						if (!hops && limit_s != Integer.MAX_VALUE && limit_o != Integer.MAX_VALUE) {
							assertTrue("describe " + q + " failed", numOfRes <= limit_o + limit_s);
						}
					}
				}
			}
		}
	}

	@Test
	public void testGetAll() throws Exception {

		for (int limit : limits) {

			if (limit != Integer.MAX_VALUE) {
				assertTrue(TestUtils.numOfRes(_tripleStore.query(queryAll, limit)) == limit);
			} else {
				assertTrue(TestUtils.numOfRes(_tripleStore.query(queryAll, limit)) == 325);
			}
		}
	}

	@Test
	public void testOSQueries() throws Exception {

		for (Value[] q : queriesOs) {

			for (int limit : limits) {

				Iterator<Statement> it = _tripleStore.query(q, limit);

				assertNotNull(it);

				int numOfRes = TestUtils.numOfRes(it);

				assertTrue(Arrays.toString(q) + " failed", numOfRes <= limit);
				assertTrue(Arrays.toString(q) + " failed. Number of result is " + numOfRes, numOfRes >= 1);
			}
		}
	}

	@Test
	public void testPOQueries() throws Exception {

		for (Value[] q : queriesPo) {

			for (int limit : limits) {

				Iterator<Statement> it = _tripleStore.query(q, limit);

				assertNotNull(it);

				int numOfRes = TestUtils.numOfRes(it);

				assertTrue(Arrays.toString(q) + " failed", numOfRes <= limit);
				assertTrue(Arrays.toString(q) + " failed", numOfRes >= 1);
			}
		}
	}

	@Test
	public void testSPQueries() throws Exception {

		for (Value[] q : queriesSp) {

			for (int limit : limits) {

				Iterator<Statement> it = _tripleStore.query(q, limit);

				assertNotNull(it);

				int numOfRes = TestUtils.numOfRes(it);

				assertTrue(Arrays.toString(q) + " failed", numOfRes <= limit);
				assertTrue(Arrays.toString(q) + " failed", numOfRes >= 1);
			}
		}
	}

	@Test
	public void testQueryNumOfResults() throws Exception {

		for (Entry<Value[], Integer> query2count : queries2count.entrySet()) {

			Iterator<Statement> it = _tripleStore.query(query2count.getKey(), Integer.MAX_VALUE);
			assertNotNull(it);
			int numOfRes = TestUtils.numOfRes(it);
			assertTrue(Arrays.toString(query2count.getKey()) + " failed", numOfRes == query2count.getValue());

		}
	}
}