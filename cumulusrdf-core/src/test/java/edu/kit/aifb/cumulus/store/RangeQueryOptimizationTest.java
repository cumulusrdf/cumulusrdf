package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.TestUtils.clean;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.memory.MemoryStore;

import edu.kit.aifb.cumulus.AbstractCumulusTest;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFSail;

public class RangeQueryOptimizationTest extends AbstractCumulusTest {

	private static Repository EMBEDDED_REPOSITORY;
	private static RepositoryConnection EMBEDDED_REPOSITORY_CONNECTION;
	private static RepositoryConnection CUMULUS_CONNECTION;

	private static final File DATAFILE = new File("src/test/resources/exp_data_resolved.nt");

	@BeforeClass
	public static void finishTest() throws Exception {
		if (EMBEDDED_REPOSITORY_CONNECTION != null) {
			EMBEDDED_REPOSITORY_CONNECTION.close();
		}
			
		if (EMBEDDED_REPOSITORY != null) {
			EMBEDDED_REPOSITORY.shutDown();
		}
	}
	@BeforeClass
	public static void setUp() throws Exception {
		// Set up standard Sesame Repository for comparison
		EMBEDDED_REPOSITORY = new SailRepository(new MemoryStore());
		EMBEDDED_REPOSITORY.initialize();
		EMBEDDED_REPOSITORY_CONNECTION = EMBEDDED_REPOSITORY.getConnection();
		EMBEDDED_REPOSITORY_CONNECTION.add(DATAFILE, null, RDFFormat.N3);

		_tripleStore = new TripleStore();
		_sail = new CumulusRDFSail(_tripleStore);
		_repository = new SailRepository(_sail);
		_repository.initialize();

		CUMULUS_CONNECTION = _repository.getConnection();
	}
	private List<String> _queries = new ArrayList<String>();
	private List<String> _orderByAscQueries = new ArrayList<String>();
	
	private List<String> _orderByDescQueries = new ArrayList<String>();

	{
		_orderByAscQueries.add("SELECT * WHERE {" + "?s <http://gridpedia.org/id/temperature> ?t ." + "FILTER(?t >= 20)" + "}" + "ORDER BY ASC(?t)" + "LIMIT 3");

		_orderByAscQueries.add("SELECT * WHERE {" + "?s <http://gridpedia.org/id/timeStamp> ?t ."
				+ "FILTER(?t >= \"2004-01-01T04:33:07Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)" + "}" + "ORDER BY ASC(?t)" + "LIMIT 15");

		_orderByAscQueries.add("SELECT * WHERE {" + "?s <http://gridpedia.org/id/temperature> ?t ." + "FILTER(?t >= 37)" + "}" + "ORDER BY ASC(?t)");

		_orderByAscQueries.add("SELECT * WHERE {" + "?s <http://gridpedia.org/id/timeStamp> ?t ."
				+ "FILTER(?t >= \"2004-01-01T04:33:07Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)" + "}" + "ORDER BY ASC(?t)");

		_orderByDescQueries.add("SELECT * WHERE {" + "?s <http://gridpedia.org/id/temperature> ?t ." + "FILTER(?t >= 37)" + "}" + "ORDER BY DESC(?t)"
				+ "LIMIT 3");

		_orderByDescQueries.add("SELECT * WHERE {" + "?s <http://gridpedia.org/id/timeStamp> ?t ."
				+ "FILTER(?t >= \"2004-01-01T04:33:07Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)" + "}" + "ORDER BY DESC(?t)" + "LIMIT 15");

		_orderByDescQueries
				.add("SELECT * WHERE {" + "?s <http://gridpedia.org/id/temperature> ?t ." + "FILTER(?t >= 37)" + "}" + "ORDER BY DESC(?t)");

		_orderByDescQueries.add("SELECT * WHERE {" + "?s <http://gridpedia.org/id/timeStamp> ?t ."
				+ "FILTER(?t >= \"2004-01-01T04:33:07Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)" + "}" + "ORDER BY DESC(?t)");

		_queries.addAll(_orderByAscQueries);
		_queries.addAll(_orderByDescQueries);

		_queries.add("SELECT * WHERE {" + "?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#value> ?o ."
				+ "?t <http://gridpedia.org/id/locationLat> ?l." + "FILTER(?l >= 10 && ?l <= 100 && ?o >= 50 && ?o <= 70)" + "}");

		_queries.add("SELECT * WHERE {" + "?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#value> ?o ." + "FILTER(?o >= 50 && ?o <= 70)" + "}");

		_queries.add("SELECT * WHERE {" + "<http://izeus1.scc.kit.edu/id/data6> <http://gridpedia.org/id/temperature> ?o ."
				+ "FILTER(?o >= -50 && ?o <= 700)" + "}");

		/*
		 * TODO: Is exception ok for "invalid" query ?
		 * _queries.add("SELECT * WHERE {" +
		 * "?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#value> ?o ." +
		 * "FILTER(?o >= 50 && ?o <= 40)" + "}");
		 */

		_queries.add("SELECT * WHERE {" + "?s <http://gridpedia.org/id/temperature> ?t ." + "FILTER(?t >= 10 && ?t <= 65)" + "}");

		_queries.add("SELECT * WHERE {"
				+ "<http://izeus1.scc.kit.edu/id/informationRelation143> <http://gridpedia.org/id/timeStamp> ?t ."
				+ "FILTER(?t >= \"2008-12-24T04:33:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> && ?t <= \"2008-12-24T04:34:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)"
				+ "}");

		_queries.add("SELECT * WHERE {" + "<http://izeus1.scc.kit.edu/id/data6> <http://gridpedia.org/id/temperature> ?o ."
				+ "FILTER(?o >= 23.5 && ?o <= 23.5)" + "}");

		_queries.add("SELECT * WHERE {"
				+ "<http://izeus1.scc.kit.edu/id/informationRelation143> <http://gridpedia.org/id/timeStamp> ?t ."
				+ "FILTER(?t >= \"2008-12-24T04:33:07Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> && ?t <= \"2008-12-24T04:33:07Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)"
				+ "}");

		_queries.add("SELECT * WHERE {" + "<http://izeus1.scc.kit.edu/id/informationRelation143> <http://gridpedia.org/id/timeStamp> ?t ."
				+ "FILTER(?t = \"2008-11-24T04:33:07Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)" + "}");

		_queries.add("SELECT * WHERE {" + "?s <http://gridpedia.org/id/temperature> ?t ." + "FILTER(?t >= 10)" + "}");

		_queries.add("SELECT * WHERE {" + "?s <http://gridpedia.org/id/temperature> ?t ." + "FILTER(?t <= 22.5)" + "}");

		_queries.add("SELECT * WHERE {"
				+ "?s <http://gridpedia.org/id/timeStamp> ?t ."
				+ "FILTER(?t >= \"2004-01-01T04:33:07Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> && ?t <= \"2004-12-31T04:33:07Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)"
				+ "}");

		_queries.add("SELECT * WHERE {"
				+ "?s <http://gridpedia.org/id/timeStamp> ?t ."
				+ "FILTER(?t >= \"2004-01-01T04:33:07Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> && ?t <= \"2004-12-31T04:33:07Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)"
				+ "}");		
	}
	
	/**
	 * Teardown fixture for each test.
	 * 
	 * @throws Exception hopefully never, otherwise the corresponding test fails.
	 */
	@After
	public void afterEachTest() throws Exception {
		clean(_tripleStore);
	}

	/**
	 * Setup fixture for each test.
	 * 
	 * @throws Exception hopefully never, otherwise the corresponding test fails.
	 */
	@Before
	public void beforeEachTest() throws Exception {
		_tripleStore.bulkLoad(DATAFILE, RDFFormat.NTRIPLES);
	}

	@Test
	public void queryOrderAscTest() throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		Iterator<String> iter = _orderByAscQueries.iterator();

		while (iter.hasNext()) {
			String query = iter.next();

			TupleQuery cq = CUMULUS_CONNECTION.prepareTupleQuery(QueryLanguage.SPARQL, query);
			TupleQueryResult cRes = cq.evaluate();
			String last = null;
			String current = null;
			while (cRes.hasNext()) {
				BindingSet bs = cRes.next();
				Iterator<Binding> bindings = bs.iterator();
				while (bindings.hasNext()) {
					Binding b = bindings.next();
					Value v = b.getValue();
					if (v instanceof Literal) {
						current = v.stringValue();
						try {
							double currDouble = Double.parseDouble(current);
							double lastDouble;
							if (last == null) {
								lastDouble = -Double.MAX_VALUE;
							} else {
								lastDouble = Double.parseDouble(last);
							}
							assertTrue(currDouble >= lastDouble);
							last = current;
						} catch (NumberFormatException ne) {
							if (last == null) {
								last = "";
							}
							assertTrue(last.compareTo(current) <= 0);
							last = current;
						}
					}
				}
			}
		}
	}

	@Test
	public void queryOrderDescTest() throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		Iterator<String> iter = _orderByDescQueries.iterator();

		while (iter.hasNext()) {
			String query = iter.next();

			TupleQuery cq = CUMULUS_CONNECTION.prepareTupleQuery(QueryLanguage.SPARQL, query);
			TupleQueryResult cRes = cq.evaluate();
			String last = null;
			String current = null;
			while (cRes.hasNext()) {
				BindingSet bs = cRes.next();
				Iterator<Binding> bindings = bs.iterator();
				while (bindings.hasNext()) {
					Binding b = bindings.next();
					Value v = b.getValue();
					if (v instanceof Literal) {
						current = v.stringValue();
						try {
							double currDouble = Double.parseDouble(current);
							double lastDouble;
							if (last == null) {
								lastDouble = Double.MAX_VALUE;
							} else {
								lastDouble = Double.parseDouble(last);
							}
							assertTrue(currDouble <= lastDouble);
							last = current;
						} catch (NumberFormatException ne) {
							if (last == null) {
								last = "_";
							}
							assertTrue(last.compareTo(current) >= 0);
							last = current;
						}
					}
				}
			}
		}
	}

	@Test
	public void queryResultsTest() throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		Iterator<String> iter = _queries.iterator();
		List<BindingSet> sesameResults;
		List<BindingSet> cumulusResults;
		while (iter.hasNext()) {

			sesameResults = new LinkedList<BindingSet>();
			cumulusResults = new LinkedList<BindingSet>();

			String query = iter.next();
			TupleQuery sq = EMBEDDED_REPOSITORY_CONNECTION.prepareTupleQuery(QueryLanguage.SPARQL, query);
			TupleQueryResult sRes = sq.evaluate();

			TupleQuery cq = CUMULUS_CONNECTION.prepareTupleQuery(QueryLanguage.SPARQL, query);
			TupleQueryResult cRes = cq.evaluate();

			while (cRes.hasNext()) {
				BindingSet bs = cRes.next();
				cumulusResults.add(bs);
			}

			while (sRes.hasNext()) {
				BindingSet bs = sRes.next();
				sesameResults.add(bs);
			}

			assertEquals(cumulusResults.size(), sesameResults.size());
			assertTrue(sesameResults.containsAll(cumulusResults));
		}
	}
}