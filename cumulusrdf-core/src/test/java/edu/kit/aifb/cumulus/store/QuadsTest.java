package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.TestUtils.buildLiteral;
import static edu.kit.aifb.cumulus.TestUtils.buildResource;
import static edu.kit.aifb.cumulus.TestUtils.clean;
import static edu.kit.aifb.cumulus.TestUtils.newQuadStore;
import static edu.kit.aifb.cumulus.TestUtils.numOfRes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFFormat;

import edu.kit.aifb.cumulus.AbstractCumulusTest;
import edu.kit.aifb.cumulus.framework.domain.configuration.ProgrammableConfigurator;

/**
 * Test case for quad pattern queries.
 * 
 * @author Sebastian Schmidt
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class QuadsTest extends AbstractCumulusTest {

	@BeforeClass
	public static void setUp() throws Exception {
		_quadStore = newQuadStore();
		_quadStore.open();
	}

	// CHECKSTYLE:OFF
	// @formatter:off
	private final Value[][] _c_queries = {
			// c -> ?s ?p ?o
			new Value[] { null, null, null, buildResource("http://dbpedia.org/data/Dorthe_Andersen.xml") },
			new Value[] { null, null, null, buildResource("http://rdf.opiumfield.com/lastfm/profile/pouryascarface") },
			new Value[] { null, null, null, buildResource("http://www.janhaeussler.com/?sioc_type=user&sioc_id=1") }
	};

	private final Value[][] _pc_queries = {
			// p -> ?s ?o ?c
			new Value[] { null, buildResource("http://xmlns.com/foaf/0.1/name"), null, null },
			new Value[] { null, buildResource("http://xmlns.com/foaf/0.1/mbox_sha1sum"), null, null },

			// p,c -> ?s ?o
			new Value[] {
					null,
					buildResource("http://xmlns.com/foaf/0.1/name"),
					null,
					buildResource("http://rdf.opiumfield.com/lastfm/profile/pouryascarface") },
					new Value[] {
					null,
					buildResource("http://purl.org/dc/elements/1.1/subject"),
					null,
					buildResource("http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E") }
	};

	private final Value[][] _spc_queries = {
			// s -> ?p ?o ?c
			new Value[] { buildResource("http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819"), null, null, null },
			new Value[] { buildResource("http://www.last.fm/user/pouryascarface"), null, null, null },

			// s,p -> ?o ?c
			new Value[] { buildResource("http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819"), RDF.TYPE, null, null },
			new Value[] {
					buildResource("http://www.last.fm/user/pouryascarface"),
					buildResource("http://www.w3.org/2002/07/owl#sameAs"),
					null,
					null },

					// s,p,o -> ?c
					new Value[] {
					buildResource("http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819"),
					RDF.TYPE,
					buildResource("http://www.rdfabout.com/rdf/schema/usbill/HouseBill"),
					null },
					new Value[] {
					buildResource("http://www.last.fm/user/pouryascarface"),
					buildResource("http://www.w3.org/2002/07/owl#sameAs"),
					buildResource("http://www.last.fm/user/pouryascarface/#avatarPanel"),
					null },

					// s,p,c -> ?o
					new Value[] {
					buildResource("http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819"),
					buildResource("http://purl.org/dc/elements/1.1/subject"),
					null,
					buildResource("http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E") },
					new Value[] {
					buildResource("http://www.last.fm/user/pouryascarface"),
					buildResource("http://www.w3.org/2000/01/rdf-schema#seeAlso"),
					null,
					buildResource("http://rdf.opiumfield.com/lastfm/profile/pouryascarface") }
	};

	private final Value[][] _oc_queries = {
			// o -> ?s ?p ?c
			new Value[] { null, null, buildResource("http://www.janhaeussler.com/author/jan-haeussler/"), null },
			new Value[] { null, null, buildLiteral("vote2"), null },

			// o,c -> ?s ?p
			new Value[] {
					null,
					null,
					buildResource("http://www.janhaeussler.com/author/jan-haeussler/"),
					buildResource("http://www.janhaeussler.com/?sioc_type=user&sioc_id=1") },
					new Value[] {
					null,
					null,
					buildLiteral("vote2"),
					buildResource("http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E") },
	};

	private final Value[][] _poc_queries = {
			// p,o -> ?s ?c
			new Value[] {
					null,
					buildResource("http://www.w3.org/2000/01/rdf-schema#seeAlso"),
					buildResource("http://rdf.opiumfield.com/lastfm/friends/pouryascarface"),
					null },
					new Value[] {
					null,
					buildResource("http://xmlns.com/foaf/0.1/primaryTopic"),
					buildResource("http://www.janhaeussler.com/author/jan-haeussler/"),
					null },

					// p,o,c -> ?s
					new Value[] {
					null,
					buildResource("http://www.w3.org/2000/01/rdf-schema#seeAlso"),
					buildResource("http://rdf.opiumfield.com/lastfm/friends/pouryascarface"),
					buildResource("http://rdf.opiumfield.com/lastfm/profile/pouryascarface") },
					new Value[] {
					null,
					buildResource("http://xmlns.com/foaf/0.1/primaryTopic"),
					buildResource("http://www.janhaeussler.com/author/jan-haeussler/"),
					buildResource("http://www.janhaeussler.com/?sioc_type=user&sioc_id=1") }
	};

	private final Value[][] _sc_queries = {
			// s,c -> ?p ?o
			new Value[] {
					buildResource("http://www.janhaeussler.com/author/jan-haeussler/#foaf"),
					null,
					null,
					buildResource("http://www.janhaeussler.com/?sioc_type=user&sioc_id=1") },
					new Value[] {
					buildResource("http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819"),
					null,
					null,
					buildResource("http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E") }
	};

	private final Value[][] _soc_queries = {
			// s,o -> ?p ?c
			new Value[] { buildResource("http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819"), null, buildLiteral("Authorization"), null },
			new Value[] { buildResource("http://www.janhaeussler.com/author/jan-haeussler/#foaf"), null, buildResource("http://www.janhaeussler.com/"), null },

			// s,o,c -> ?p
			new Value[] {
					buildResource("http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819"),
					null,
					buildLiteral("Authorization"),
					buildResource("http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E") },
					new Value[] {
					buildResource("http://www.janhaeussler.com/author/jan-haeussler/#foaf"),
					null,
					buildResource("http://www.janhaeussler.com/"),
					buildResource("http://www.janhaeussler.com/?sioc_type=user&sioc_id=1") },

	};

	// s,p,o,c
	private final Value[][] _spoc_queries = {
			new Value[] {
					buildResource("http://www.janhaeussler.com/?sioc_type=user&sioc_id=1"),
					buildResource("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
					buildResource("http://xmlns.com/foaf/0.1/Document"),
					buildResource("http://www.janhaeussler.com/?sioc_type=user&sioc_id=1") }
	};

	// ?s,?p,?o,?c
	private final Value[][] _null_queries = new Value[1][4];
	private Map<Value[], Integer> _query2numOfRes = new HashMap<Value[], Integer>();

	{
		_query2numOfRes.put(_c_queries[0], 2);
		_query2numOfRes.put(_c_queries[1], 18);
		_query2numOfRes.put(_c_queries[2], 15);

		_query2numOfRes.put(_pc_queries[0], 2);
		_query2numOfRes.put(_pc_queries[1], 2);
		_query2numOfRes.put(_pc_queries[2], 1);
		_query2numOfRes.put(_pc_queries[3], 13);

		_query2numOfRes.put(_spc_queries[0], 30);
		_query2numOfRes.put(_spc_queries[1], 14);
		_query2numOfRes.put(_spc_queries[2], 1);
		_query2numOfRes.put(_spc_queries[3], 1);
		_query2numOfRes.put(_spc_queries[4], 1);
		_query2numOfRes.put(_spc_queries[5], 1);
		_query2numOfRes.put(_spc_queries[6], 13);
		_query2numOfRes.put(_spc_queries[7], 5);

		_query2numOfRes.put(_oc_queries[0], 2);
		_query2numOfRes.put(_oc_queries[1], 1);
		_query2numOfRes.put(_oc_queries[2], 2);
		_query2numOfRes.put(_oc_queries[3], 1);

		_query2numOfRes.put(_poc_queries[0], 1);
		_query2numOfRes.put(_poc_queries[1], 1);
		_query2numOfRes.put(_poc_queries[2], 1);
		_query2numOfRes.put(_poc_queries[3], 1);

		_query2numOfRes.put(_sc_queries[0], 7);
		_query2numOfRes.put(_sc_queries[1], 30);

		_query2numOfRes.put(_soc_queries[0], 1);
		_query2numOfRes.put(_soc_queries[1], 1);
		_query2numOfRes.put(_soc_queries[2], 1);
		_query2numOfRes.put(_soc_queries[3], 1);

		_query2numOfRes.put(_spoc_queries[0], 1);

		_query2numOfRes.put(_null_queries[0], 100);
	}
	// @formatter:off
	// CHECKSTYLE:ON

	/**
	 * Internal method used by several test to assert that a set of queries produces correct results.
	 * 
	 * @param queries the set of queries to be checked.
	 * @throws Exception never, otherwise the corresponding test will fail.
	 */
	private void assertQuery(final Value[][] queries) throws Exception {

		for (final Value[] query : queries) {

			final Iterator<Statement> iterator = _quadStore.query(query);

			assertNotNull(iterator);
			assertEquals("query: " + Arrays.toString(query), (int) _query2numOfRes.get(query), numOfRes(iterator));
		}
	}

	@Before
	public void loadTestData() throws Exception {
		_quadStore.bulkLoad(DATA_NQ, RDFFormat.NQUADS);
		assertEquals((int) _query2numOfRes.get(_null_queries[0]), numOfRes(_quadStore.query(_null_queries[0])));
	}

	@After
	public void removeTestData() throws Exception {
		clean(_quadStore);
	}

	@Test
	public void testCQuery() throws Exception {
		assertQuery(_c_queries);
	}

	@Test
	public void testNullQuery() throws Exception {
		assertQuery(_null_queries);
	}

	@Test
	public void testOCQuery() throws Exception {
		assertQuery(_oc_queries);
	}

	@Test
	public void testPCQuery() throws Exception {
		assertQuery(_pc_queries);
	}

	@Test
	public void testPOCQuery() throws Exception {
		assertQuery(_poc_queries);
	}

	@Test
	public void testSCQuery() throws Exception {
		assertQuery(_sc_queries);
	}

	@Test
	public void testSOCQuery() throws Exception {
		assertQuery(_soc_queries);
	}

	@Test
	public void testSPCQuery() throws Exception {
		assertQuery(_spc_queries);
	}

	@Test
	public void testSPOCQuery() throws Exception {
		assertQuery(_spoc_queries);
	}
	
	/**
	 * Test if the ttl feature for the triple store works.
	 * @throws Exception never, otherwise the corresponding test will fail.
	 */
	@Ignore
	@Test
	public void testTTL() throws Exception {
		ProgrammableConfigurator progConf = new ProgrammableConfigurator(Collections.singletonMap("ttl-value", Integer.valueOf(5))); 
		QuadStore quadStore = new QuadStore("TestKS");
		quadStore.accept(progConf);
		quadStore.open();
		  
		quadStore.bulkLoad(DATA_NQ, RDFFormat.NQUADS);
		assertEquals((int) _query2numOfRes.get(_null_queries[0]), numOfRes(quadStore.query(_null_queries[0])));
		  
		Thread.sleep(7000);
		  
		assertEquals(0, numOfRes(quadStore.query(_null_queries[0])));
	}
}