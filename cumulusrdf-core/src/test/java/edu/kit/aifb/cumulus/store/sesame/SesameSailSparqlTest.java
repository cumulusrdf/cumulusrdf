package edu.kit.aifb.cumulus.store.sesame;

import static edu.kit.aifb.cumulus.TestUtils.clean;
import static edu.kit.aifb.cumulus.TestUtils.newTripleStore;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.resultio.UnsupportedQueryResultFormatException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.SailException;

import edu.kit.aifb.cumulus.AbstractCumulusTest;
import edu.kit.aifb.cumulus.TestUtils;
import edu.kit.aifb.cumulus.store.CumulusStoreException;


public class SesameSailSparqlTest extends AbstractCumulusTest {
	
	@BeforeClass
	public static void setUp() throws Exception {
		_tripleStore = newTripleStore();
		_sail = new CumulusRDFSail(_tripleStore);
		_sail.initialize();
	}

	@Before
	public void beforeEachTest() throws Exception {
		_tripleStore.bulkLoad(DATA_NT, RDFFormat.NTRIPLES);		
	}
	
	/**
	 * Teardown fixture for each test.
	 * Basically deletes all data in the store.
	 * 
	 * @throws Exception hopefully never, otherwise the corresponding test fails.
	 */
	@After
	public void afterEachTest() throws Exception {
		clean(_tripleStore);
	}	
	
	@Test
	public void testQueries() throws CumulusStoreException, SailException, RepositoryException, MalformedQueryException, QueryEvaluationException {

		RepositoryConnection repoConn = new SailRepository(_sail).getConnection();

		for (String query_strg : SPARQL_QUERIES) {

			Query query = repoConn.prepareQuery(QueryLanguage.SPARQL, query_strg);

			if (query instanceof BooleanQuery) {

				boolean qres = ((BooleanQuery) query).evaluate();

				try {

					Assert.assertTrue(query_strg, qres);

				} catch (UnsupportedQueryResultFormatException e) {
					e.printStackTrace();
				}
			} else if (query instanceof TupleQuery) {

				TupleQueryResult qres = ((TupleQuery) query).evaluate();

				try {

					Assert.assertNotNull(query_strg, qres);
					Assert.assertEquals(query_strg, (int) QUERIES_2_COUNT.get(query_strg), TestUtils.numOfRes(qres));

				} catch (UnsupportedQueryResultFormatException e) {
					e.printStackTrace();
				}

				qres.close();

			} else if (query instanceof GraphQuery) {

				GraphQueryResult qres = ((GraphQuery) query).evaluate();

				Assert.assertNotNull(query_strg, qres);
				Assert.assertEquals(query_strg, (int) QUERIES_2_COUNT.get(query_strg), TestUtils.numOfRes(qres));

				qres.close();
			}
		}

		repoConn.close();
	}
}