package edu.kit.aifb.cumulus.store.sesame;

import static edu.kit.aifb.cumulus.TestUtils.clean;
import static edu.kit.aifb.cumulus.TestUtils.newQuadStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.resultio.UnsupportedQueryResultFormatException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;

import edu.kit.aifb.cumulus.AbstractCumulusTest;
import edu.kit.aifb.cumulus.TestUtils;

/**
 * CumulusRDF Sesame Sail Quad Test.
 * 
 * @author Andreas Wagner
 * since 1.0
 */
public class SesameSailQuadsTest extends AbstractCumulusTest {

	private static final List<String> SPARQL_QUAD_QUERIES = new ArrayList<String>();
	private static final List<String> SPARQL_QUAD_ASK = new ArrayList<String>();
	private static final List<String> SPARQL_QUAD_SELECT = new ArrayList<String>();
	private static final List<String> SPARQL_QUAD_CONSTRUCT = new ArrayList<String>();
		
	// CHECKSTYLE:OFF
	static {
		SPARQL_QUAD_ASK.add("ASK FROM <http://www.janhaeussler.com/?sioc_type=user&sioc_id=1> { ?s ?p ?o . }");
		SPARQL_QUAD_ASK.add("ASK FROM <http://www.janhaeussler.com/?sioc_type=user&sioc_id=1> { <http://www.janhaeussler.com/author/jan-haeussler/#foaf> <http://xmlns.com/foaf/0.1/name> ?o . }");
		SPARQL_QUAD_ASK.add("ASK FROM <http://rdf.opiumfield.com/lastfm/profile/pouryascarface> { ?s <http://xmlns.com/foaf/0.1/age> \"28\" . }");
		SPARQL_QUAD_ASK.add("ASK FROM <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> { <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://purl.org/dc/elements/1.1/subject> \"Budgets\" . }");
		SPARQL_QUAD_ASK.add("ASK FROM <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> { ?s <http://purl.org/dc/elements/1.1/subject> \"Authorization\" . ?s <http://purl.org/dc/elements/1.1/subject> \"Congress\" . }");
		SPARQL_QUAD_QUERIES.addAll(SPARQL_QUAD_ASK);
		
		SPARQL_QUAD_SELECT.add("SELECT * FROM <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> WHERE { <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://purl.org/dc/elements/1.1/subject> ?o . }");
		QUERIES_2_COUNT.put(SPARQL_QUAD_SELECT.get(0), 13);
		SPARQL_QUAD_SELECT.add("SELECT * FROM <http://vijole.blogiem.lv/comments_rss.rdf> WHERE { <http://vijole.blogiem.lv/comments_rss.rdf> ?p ?o . }");
		QUERIES_2_COUNT.put(SPARQL_QUAD_SELECT.get(1), 7);
		SPARQL_QUAD_SELECT.add("SELECT ?s FROM <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> WHERE { ?s <http://purl.org/dc/elements/1.1/subject> \"Independent regulatory commissions\" . ?s <http://purl.org/dc/elements/1.1/subject> \"Presidential appointments\" . }");
		QUERIES_2_COUNT.put(SPARQL_QUAD_SELECT.get(2), 1);
		SPARQL_QUAD_QUERIES.addAll(SPARQL_QUAD_SELECT);
		
		SPARQL_QUAD_CONSTRUCT.add("CONSTRUCT FROM <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> { <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://purl.org/dc/elements/1.1/subject> ?o } ");
		QUERIES_2_COUNT.put(SPARQL_QUAD_CONSTRUCT.get(0), 13);
		SPARQL_QUAD_CONSTRUCT.add("CONSTRUCT { <http://vijole.blogiem.lv/comments_rss.rdf> ?p ?o . } FROM <http://vijole.blogiem.lv/comments_rss.rdf> WHERE { ?s ?p ?o . FILTER isLITERAL(?o) } ");
		QUERIES_2_COUNT.put(SPARQL_QUAD_CONSTRUCT.get(1), 3);
		SPARQL_QUAD_CONSTRUCT.add("CONSTRUCT { <http://www.rdfabout.com/rdf/usgov/congress/106/bills/h819> <http://www.rdfabout.com/rdf/schema/usbill/inCommittee> ?o } FROM <http://www.rdfabout.com/sparql?query=DESCRIBE+%3Chttp://www.rdfabout.com/rdf/usgov/congress/106/bills/h819%3E> WHERE { ?o <http://www.w3.org/2000/01/rdf-schema#label> \"House Committee on Transportation and Infrastructure: Subcommittee on Coast Guard and Maritime Transportation\" }");
		QUERIES_2_COUNT.put(SPARQL_QUAD_CONSTRUCT.get(2), 1);
		SPARQL_QUAD_QUERIES.addAll(SPARQL_QUAD_CONSTRUCT);
	}
	// CHECKSTYLE:ON
	
	/**
	 * Setup general fixture for this test case.
	 * 
	 * @throws Exception never, otherwise no test will run.
	 */
	@BeforeClass
	public static void beforeAllTests() throws Exception {
		_quadStore = newQuadStore();
		
		_sail = new CumulusRDFSail(_quadStore);
		_sail.initialize();
		_repository = new SailRepository(_sail);
	}
	
	/**
	 * Teardown fixture for each test.
	 * 
	 * @throws Exception hopefully never, otherwise the corresponding test fails.
	 */
	@After
	public void afterEachTest() throws Exception {
		clean(_quadStore);
	}

	/**
	 * Setup fixture for each test.
	 * 
	 * @throws Exception hopefully never, otherwise the corresponding test fails.
	 */
	@Before
	public void beforeEachTest() throws Exception {
		RepositoryConnection connection = _repository.getConnection();		
		connection.add(new File(DATA_NQ), null, RDFFormat.NQUADS);		
		connection.close();
	}
	
	/**
	 * Several test queries.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void testQueries() throws Exception {

		RepositoryConnection repoConn = new SailRepository(_sail).getConnection();

		for (String query_strg : SPARQL_QUAD_QUERIES) {

			Query query = repoConn.prepareQuery(QueryLanguage.SPARQL, query_strg);

			if (query instanceof BooleanQuery) {

				boolean qres = ((BooleanQuery) query).evaluate();

				try {

					assertTrue(query_strg, qres);

				} catch (UnsupportedQueryResultFormatException e) {
					e.printStackTrace();
				}
			} else if (query instanceof TupleQuery) {

				TupleQueryResult qres = ((TupleQuery) query).evaluate();

				try {

					assertNotNull(query_strg, qres);
					assertEquals(query_strg, (int) QUERIES_2_COUNT.get(query_strg), TestUtils.numOfRes(qres));

				} catch (UnsupportedQueryResultFormatException e) {
					e.printStackTrace();
				}

				qres.close();

			} else if (query instanceof GraphQuery) {

				GraphQueryResult qres = ((GraphQuery) query).evaluate();

				assertNotNull(query_strg, qres);
				assertEquals(query_strg, (int) QUERIES_2_COUNT.get(query_strg), TestUtils.numOfRes(qres));

				qres.close();
			}
		}

		repoConn.close();
	}
}