/*
 * This Test case makes use of some examples from 
 * 
 * "Learning SPARQL - Querying and Updating with SPARQL 1.1" by Bob DuCharme
 * 
 * Publisher: O'Reilly
 * Author: Bob DuCharme
 * ISBN: 978-1-449-37143-2
 * http://www.learningsparql.com/
 * http://shop.oreilly.com/product/0636920030829.do
 * 
 * We warmly appreciate and thank the author and O'Reilly for such permission.
 * 
 */
package edu.kit.aifb.cumulus.webapp;

import static edu.kit.aifb.cumulus.WebTestUtils.buildLiteral;
import static edu.kit.aifb.cumulus.WebTestUtils.buildResource;
import static edu.kit.aifb.cumulus.WebTestUtils.clean;
import static edu.kit.aifb.cumulus.WebTestUtils.newTripleStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import edu.kit.aifb.cumulus.AbstractCumulusWebTest;
import edu.kit.aifb.cumulus.StubServletOutputStream;
import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.framework.Environment.ConfigValues;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFSail;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Headers;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Methods;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Parameters;

/**
 * Test cases for the {@link SPARQLServlet}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SPARQLServletTest2 extends AbstractCumulusWebTest {

	protected SPARQLServlet _classUnderTest;

	protected static final String EXAMPLES_DIR = "src/test/resources/examples_from_learning_sparql/";

	/**
	 * Setup fixture for all tests.
	 * 
	 * @throws Exception never otherwise tests fail.
	 */
	@BeforeClass
	public static void beforeAllTests() throws Exception {
		TRIPLE_STORE = newTripleStore();
		SAIL = new CumulusRDFSail(TRIPLE_STORE);
		REPOSITORY = new SailRepository(SAIL);
		REPOSITORY.initialize();
	}

	private final String[] _accept_select_header = {
			TupleQueryResultFormat.BINARY.getDefaultMIMEType(),
			TupleQueryResultFormat.SPARQL.getDefaultMIMEType(),
			TupleQueryResultFormat.JSON.getDefaultMIMEType(),
			TupleQueryResultFormat.TSV.getDefaultMIMEType(),
			TupleQueryResultFormat.CSV.getDefaultMIMEType()
	};

	protected ServletContext _context;

	/**
	 * Shortcut utility for creating expected binding sets.
	 * 
	 * @param expected the list that accumulates expected results.
	 * @param pairs the binding set name / value pairs.
	 */
	private void addExpectedBindingSet(final List<BindingSet> expected, final Object[]... pairs) {
		if (pairs == null) {
			return;
		}

		final MapBindingSet bindingSet = new MapBindingSet(pairs.length);

		for (int i = 0; i < pairs.length; i++) {
			final Object[] pair = pairs[i];
			if (pair.length != 2) {
				fail("Wrong pair length.");
			}

			bindingSet.addBinding(String.valueOf(pair[0]), (Value) pair[1]);
		}
		expected.add(bindingSet);
	}

	/**
	 * Shortcut utility for creating expected binding sets.
	 * 
	 * @param expected the list that accumulates expected results.
	 * @param pairs the binding set name / value pairs.
	 */
	private void addExpectedBindingSet(final List<BindingSet> expected, final String... pairs) {
		if ((pairs == null) || (pairs.length % 2 != 0)) {
			fail("Varags are supposed to be name/value pairs...");
		}

		final MapBindingSet bindingSet = new MapBindingSet(pairs.length / 2);
		for (int i = 0; i < pairs.length; i += 2) {
			bindingSet.addBinding(pairs[i], buildLiteral(pairs[i + 1]));
		}
		expected.add(bindingSet);
	}

	/**
	 * Teardown fixture for each test.
	 * Basically deletes all data in the store.
	 * 
	 * @throws Exception hopefully never, otherwise the corresponding test fails.
	 */
	@After
	public void afterEachTest() throws Exception {
		clean(TRIPLE_STORE);
	}

	/**
	 * Asserts the correctness of a given tuple result against an expected set of binding sets.
	 * 
	 * @param expected the expected set of binding sets.
	 * @param result the tuple query result.
	 * @throws QueryEvaluationException in case of failure while iterating over the result.
	 */
	private void assertTupleResultCorrectness(final List<BindingSet> expected, final TupleQueryResult result) throws QueryEvaluationException {
		int rows = 0;
		int expectedRows = expected.size();

		while (result.hasNext()) {
			rows++;
			BindingSet set = result.next();
			expected.remove(set);
		}

		assertEquals(expectedRows, rows);
		assertEquals(0, expected.size());
	}

	/**
	 * Setup fixture for each test.
	 * Initializes mock stuff and load sample data.
	 * 
	 * @throws Exception hopefully never, otherwise the corresponding test fails.
	 */
	@Before
	public void beforeEachTest() throws Exception {
		_context = mock(ServletContext.class);
		when(_context.getAttribute(ConfigParams.SESAME_REPO)).thenReturn(REPOSITORY);
		when(_context.getAttribute(ConfigParams.STORE)).thenReturn(TRIPLE_STORE);
		when(_context.getAttribute(ConfigParams.LAYOUT)).thenReturn(ConfigValues.STORE_LAYOUT_TRIPLE);

		_classUnderTest = spy(new SPARQLServlet());
		doReturn(_context).when(_classUnderTest).getServletContext();
		when(_context.getNamedDispatcher(anyString())).thenReturn(mock(RequestDispatcher.class));
	}

	/**
	 * Creates a mock {@link HttpServletRequest}.
	 *
	 * @param query the SPARQL query.
	 * @param update the SPARQL update query.
	 * @param acceptParameter the accept value as request parameter.
	 * @param acceptHeader the accept value as request header.
	 * @return a mock {@link HttpServletRequest}.
	 */
	protected HttpServletRequest createMockHttpRequest(
			final String query,
			final String acceptParameter,
			final String acceptHeader,
			final String update) {

		final HttpServletRequest request = mock(HttpServletRequest.class);
		when(request.getParameter(Parameters.QUERY)).thenReturn(query);
		when(request.getHeader(Headers.ACCEPT)).thenReturn(acceptHeader);
		when(request.getParameter(Parameters.UPDATE)).thenReturn(update, "UTF-8");
		when(request.getMethod()).thenReturn(Methods.POST);

		return request;
	}

	/**
	 * A simple query with just one triple pattern and prefixes.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex003() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"craigEmail", "craigellis@yahoo.com");

		addExpectedBindingSet(
				expected,
				"craigEmail", "c.ellis@usairwaysgroup.com");

		testQuery("ex002.ttl", "ex003.rq", expected);
	}

	/**
	 * A simple query with just one triple pattern without prefixes.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex006() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"craigEmail", "craigellis@yahoo.com");

		addExpectedBindingSet(
				expected,
				"craigEmail", "c.ellis@usairwaysgroup.com");

		testQuery("ex002.ttl", "ex006.rq", expected);
	}

	/**
	 * A simple query with just one triple pattern (subject is a variable).
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex008() throws Exception {
		for (final String format : _accept_select_header) {
			final TupleQueryResult results = executeQuery("ex002.ttl", "ex008.rq", format);
			assertTrue(results.hasNext());
			final BindingSet result = results.next();
			assertEquals(1, result.size());

			final Binding binding = result.getBinding("person");
			assertEquals(buildResource("http://learningsparql.com/ns/addressbook#richard"), binding.getValue());
			assertFalse(results.hasNext());
		}
	}

	/**
	 * A simple query with just one triple pattern (predicate and object are variables).
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex010() throws Exception {
		for (final String format : _accept_select_header) {

			final Map<Value, Value> expectedResults = new HashMap<Value, Value>();
			expectedResults.put(buildResource("http://learningsparql.com/ns/addressbook#email"), buildLiteral("cindym@gmail.com"));
			expectedResults.put(buildResource("http://learningsparql.com/ns/addressbook#homeTel"), buildLiteral("(245) 646-5488"));

			final TupleQueryResult results = executeQuery("ex002.ttl", "ex010.rq", format);

			while (results.hasNext()) {
				final BindingSet result = results.next();
				assertEquals(2, result.size());

				final Binding propertyName = result.getBinding("propertyName");
				final Binding propertyValue = result.getBinding("propertyValue");
				assertEquals(expectedResults.remove(propertyName.getValue()), propertyValue.getValue());
			}

			assertEquals("Missing one or more expected result " + expectedResults, 0, expectedResults.size());
		}
	}

	/**
	 * A query with two triple patterns (i.e. a simple graph pattern).
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex013() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"craigEmail", "craigellis@yahoo.com");

		addExpectedBindingSet(
				expected,
				"craigEmail", "c.ellis@usairwaysgroup.com");

		testQuery("ex012.ttl", "ex013.rq", expected);
	}

	/**
	 * A query with three triple patterns.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex015() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"craigEmail", "craigellis@yahoo.com");

		addExpectedBindingSet(
				expected,
				"craigEmail", "c.ellis@usairwaysgroup.com");

		testQuery("ex012.ttl", "ex013.rq", expected);
	}

	//	/**
	//	 * A query that makes use of ; for abbreviating triple patterns.
	//	 * 
	//	 * @throws Exception never, otherwise test fails.
	//	 */
	//	@Test
	//	public void ex047() throws Exception {
	//		loadData("rdf.xml", RDFFormat.RDFXML);
	//		loadData("rdfs.xml", RDFFormat.RDFXML);
	//		loadData("owl.ttl");
	//			
	//		for (final String format : _accept_select_header) {	
	//			
	//			final TupleQueryResult results = executeQuery("ex046.ttl", "ex047a.rq", format);
	//
	//			assertTrue(results.hasNext());
	//			
	//			final BindingSet result = results.next();
	//			assertEquals(4, result.size());
	//
	//			assertEquals(result.getBinding("doctorFirst").getValue(), buildLiteral("Richard"));
	//			assertEquals(result.getBinding("doctorLast").getValue(), buildLiteral("Mutt"));
	//			assertEquals(result.getBinding("spouseFirst").getValue(), buildLiteral("Cindy"));
	//			assertEquals(result.getBinding("spouseLast").getValue(), buildLiteral("Marshall"));
	//			
	//			assertFalse(results.hasNext());
	//		}
	//	}		

	/**
	 * A query with three triple patterns and two variables (human readable labels).
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex017() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"first", "Richard",
				"last", "Mutt");

		testQuery("ex012.ttl", "ex017.rq", expected);
	}

	/**
	 * Find all about Cindy.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex019() throws Exception {
		for (final String format : _accept_select_header) {

			final Map<Value, Value> expectedResults = new HashMap<Value, Value>();
			expectedResults.put(buildResource("http://learningsparql.com/ns/addressbook#email"), buildLiteral("cindym@gmail.com"));
			expectedResults.put(buildResource("http://learningsparql.com/ns/addressbook#homeTel"), buildLiteral("(245) 646-5488"));
			expectedResults.put(buildResource("http://learningsparql.com/ns/addressbook#lastName"), buildLiteral("Marshall"));
			expectedResults.put(buildResource("http://learningsparql.com/ns/addressbook#firstName"), buildLiteral("Cindy"));

			final TupleQueryResult results = executeQuery("ex012.ttl", "ex019.rq", format);

			while (results.hasNext()) {
				final BindingSet result = results.next();
				assertEquals(2, result.size());

				final Binding propertyName = result.getBinding("propertyName");
				final Binding propertyValue = result.getBinding("propertyValue");
				assertEquals(expectedResults.remove(propertyName.getValue()), propertyValue.getValue());
			}

			assertEquals("Missing one or more expected result " + expectedResults, 0, expectedResults.size());
		}
	}

	/**
	 * A query for every bound variables (*) with a FILTER keyword.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex021() throws Exception {
		for (final String format : _accept_select_header) {

			final TupleQueryResult results = executeQuery("ex012.ttl", "ex021.rq", format);

			assertTrue(results.hasNext());

			final BindingSet result = results.next();
			assertEquals(3, result.size());

			assertEquals(result.getBinding("s").getValue(), buildResource("http://learningsparql.com/ns/data#i8301"));
			assertEquals(result.getBinding("p").getValue(), buildResource("http://learningsparql.com/ns/addressbook#email"));
			assertEquals(result.getBinding("o").getValue(), buildLiteral("craigellis@yahoo.com"));

			assertFalse(results.hasNext());
		}
	}

	/**
	 * A query with an unmatching graph pattern.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex023() throws Exception {
		for (final String format : _accept_select_header) {

			final TupleQueryResult results = executeQuery("ex012.ttl", "ex023.rq", format);

			assertFalse(results.hasNext());
		}
	}

	/**
	 * Querying data and ontology (FOAF).
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex052() throws Exception {
		loadData("foaf.xml", RDFFormat.RDFXML);

		for (final String format : _accept_select_header) {
			final Map<Value, Value> expectedResult = new HashMap<Value, Value>();
			expectedResult.put(buildLiteral("personal mailbox"), buildLiteral("richard49@hotmail.com"));
			expectedResult.put(buildLiteral("nickname"), buildLiteral("Dick"));
			expectedResult.put(buildLiteral("Surname"), buildLiteral("Mutt"));
			expectedResult.put(buildLiteral("Given name"), buildLiteral("Richard"));
			expectedResult.put(buildLiteral("workplace homepage"), buildResource("http://www.philamuseum.org/"));
			expectedResult.put(buildLiteral("AIM chat ID"), buildLiteral("bridesbachelor"));

			final TupleQueryResult results = executeQuery("ex050.ttl", "ex052.rq", format);

			while (results.hasNext()) {
				final BindingSet result = results.next();
				assertEquals(2, result.size());
				assertEquals(
						expectedResult.remove(result.getBinding("propertyLabel").getValue()),
						result.getBinding("value").getValue());
			}

			assertEquals("Missing one or more expected result " + expectedResult, 0, expectedResult.size());
		}
	}

	/**
	 * Multiple triple patterns.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex055() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"first", "Craig",
				"last", "Ellis",
				"workTel", "(245) 315-5486");

		testQuery("ex054.ttl", "ex055.rq", expected);
	}

	/**
	 * OPTIONAL keyword with one triple pattern.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex057() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"first", "Craig",
				"last", "Ellis",
				"workTel", "(245) 315-5486");

		addExpectedBindingSet(
				expected,
				"first", "Cindy",
				"last", "Marshall");

		addExpectedBindingSet(
				expected,
				"first", "Richard",
				"last", "Mutt");

		testQuery("ex054.ttl", "ex057.rq", expected);
	}

	/**
	 * OPTIONAL keyword with two triple patterns.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex059() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"first", "Craig",
				"last", "Ellis");

		addExpectedBindingSet(
				expected,
				"first", "Cindy",
				"last", "Marshall");

		addExpectedBindingSet(
				expected,
				"first", "Richard",
				"last", "Mutt");

		testQuery("ex054.ttl", "ex059.rq", expected);
	}

	/**
	 * Two OPTIONAL clauses each with one triple pattern.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex061() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"first", "Craig",
				"last", "Ellis",
				"workTel", "(245) 315-5486");

		addExpectedBindingSet(
				expected,
				"first", "Cindy",
				"last", "Marshall");

		addExpectedBindingSet(
				expected,
				"first", "Richard",
				"last", "Mutt",
				"nick", "Dick");

		testQuery("ex054.ttl", "ex061.rq", expected);
	}

	/**
	 * Order of OPTIONAL graph pattern matters.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex063() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>(3);

		addExpectedBindingSet(
				expected,
				"first", "Craig",
				"last", "Ellis");

		addExpectedBindingSet(
				expected,
				"first", "Cindy",
				"last", "Marshall");

		addExpectedBindingSet(
				expected,
				"first", "Dick",
				"last", "Mutt");

		testQuery("ex054.ttl", "ex063.rq", expected);
	}

	/**
	 * FILTER keyword with a !bound in order to find data that doesn't meet a condition.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex065() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>(3);

		addExpectedBindingSet(
				expected,
				"first", "Cindy",
				"last", "Marshall");

		addExpectedBindingSet(
				expected,
				"first", "Richard",
				"last", "Mutt");

		testQuery("ex054.ttl", "ex065.rq", expected);
	}

	/**
	 * SPARQL 1.1 FILTER NOT EXISTS in order to find data that doesn't meet a condition.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex067() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"first", "Cindy",
				"last", "Marshall");

		addExpectedBindingSet(
				expected,
				"first", "Richard",
				"last", "Mutt");

		testQuery("ex054.ttl", "ex067.rq", expected);
	}

	/**
	 * MINUS keyword in order to find data that doesn't meet a condition.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex068() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"first", "Cindy",
				"last", "Marshall");

		addExpectedBindingSet(
				expected,
				"first", "Richard",
				"last", "Mutt");

		testQuery("ex054.ttl", "ex068.rq", expected);
	}

	/**
	 * Simple join.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex070() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"last", "Marshall",
				"first", "Cindy",
				"courseName", "Modeling Data with OWL");

		addExpectedBindingSet(
				expected,
				"last", "Marshall",
				"first", "Cindy",
				"courseName", "Using SPARQL with non-RDF Data");

		addExpectedBindingSet(
				expected,
				"last", "Ellis",
				"first", "Craig",
				"courseName", "Using SPARQL with non-RDF Data");

		addExpectedBindingSet(
				expected,
				"last", "Mutt",
				"first", "Richard",
				"courseName", "Using SPARQL with non-RDF Data");

		addExpectedBindingSet(
				expected,
				"last", "Mutt",
				"first", "Richard",
				"courseName", "Updating Data with SPARQL");

		testQuery("ex069.ttl", "ex070.rq", expected);
	}

	/**
	 * Different predicates to express the same concept.
	 * The usage of | operator makes possible to "normalize" the resulting tuples.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex075() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperB") },
				new Object[] { "title", buildLiteral("Paper B") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperA") },
				new Object[] { "title", buildLiteral("Paper A") });

		testQuery("ex074.ttl", "ex075.rq", expected);
	}

	/**
	 * Different predicates to express the same concept.
	 * The usage of | operator makes possible to "normalize" the resulting tuples.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex077() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperB") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperC") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperD") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperE") });

		testQuery("ex074.ttl", "ex077.rq", expected);
	}

	/**
	 * Simple property path expression (+).
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex078() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperE") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperG") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperI") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperF") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperD") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperH") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperC") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperB") });

		testQuery("ex074.ttl", "ex078.rq", expected);
	}

	/**
	 * Property path.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex082() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		for (int i = 0; i < 4; i++) {

			addExpectedBindingSet(
					expected,
					new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperI") });
		}

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperH") });

		testQuery("ex074.ttl", "ex082.rq", expected);
	}

	/**
	 * Simple inverse property path.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex083() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperB") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperC") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperD") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperE") });

		testQuery("ex074.ttl", "ex083.rq", expected);
	}

	/**
	 * Complex inverse property path.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex084() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperG") });

		addExpectedBindingSet(
				expected,
				new Object[] { "s", buildResource("http://learningsparql.com/ns/papers#paperG") });

		testQuery("ex074.ttl", "ex084.rq", expected);
	}

	/**
	 * Query on blank nodes.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex088() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"firstName", "Richard",
				"lastName", "Mutt",
				"streetAddress", "32 Main St.",
				"city", "Springfield",
				"region", "Connecticut",
				"postalCode", "49345");

		testQuery("ex041.ttl", "ex088.rq", expected);
	}

	/**
	 * Duplicates.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex090() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				new Object[] { "p", buildResource("http://learningsparql.com/ns/addressbook#takingCourse") });

		for (int i = 0; i < 4; i++) {
			addExpectedBindingSet(
					expected,
					new Object[] { "p", buildResource("http://learningsparql.com/ns/addressbook#courseTitle") });

			addExpectedBindingSet(
					expected,
					new Object[] { "p", buildResource("http://learningsparql.com/ns/addressbook#takingCourse") });

			if (i > 0) {
				addExpectedBindingSet(
						expected,
						new Object[] { "p", buildResource("http://learningsparql.com/ns/addressbook#email") });

				addExpectedBindingSet(
						expected,
						new Object[] { "p", buildResource("http://learningsparql.com/ns/addressbook#lastName") });

				addExpectedBindingSet(
						expected,
						new Object[] { "p", buildResource("http://learningsparql.com/ns/addressbook#firstName") });
			}
		}

		testQuery("ex069.ttl", "ex090.rq", expected);
	}

	/**
	 * Remove duplicates with DISTINCT keyword.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex092() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				new Object[] { "p", buildResource("http://learningsparql.com/ns/addressbook#courseTitle") });

		addExpectedBindingSet(
				expected,
				new Object[] { "p", buildResource("http://learningsparql.com/ns/addressbook#takingCourse") });

		addExpectedBindingSet(
				expected,
				new Object[] { "p", buildResource("http://learningsparql.com/ns/addressbook#email") });

		addExpectedBindingSet(
				expected,
				new Object[] { "p", buildResource("http://learningsparql.com/ns/addressbook#lastName") });

		addExpectedBindingSet(
				expected,
				new Object[] { "p", buildResource("http://learningsparql.com/ns/addressbook#firstName") });

		testQuery("ex069.ttl", "ex092.rq", expected);
	}

	/**
	 * Remove duplicates with DISTINCT keyword.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void ex094() throws Exception {
		final List<BindingSet> expected = new ArrayList<BindingSet>();

		addExpectedBindingSet(
				expected,
				"first", "Craig",
				"last", "Ellis");

		addExpectedBindingSet(
				expected,
				"first", "Cindy",
				"last", "Marshall");

		addExpectedBindingSet(
				expected,
				"first", "Richard",
				"last", "Mutt");

		testQuery("ex069.ttl", "ex094.rq", expected);
	}

	/**
	 * Internal method used for query execution.
	 * 
	 * @param dataFilename the name of the file containing data.
	 * @param queryFilename the name of the file containing query.
	 * @param format the format name that will be used in returning result.
	 * @return the result of query evaluation against the given dataset.
	 * @throws Exception never, otherwise the corresponding test fails.
	 */
	private TupleQueryResult executeQuery(final String dataFilename, final String queryFilename, final String format) throws Exception {
		loadData(dataFilename);

		final String query = readQuery(queryFilename);

		final String accept = TupleQueryResultFormat.SPARQL.getDefaultMIMEType();

		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		final ServletOutputStream servletOutputStream = new StubServletOutputStream(output);

		final HttpServletRequest request = createMockHttpRequest(query, null, accept, null);
		final HttpServletResponse response = mock(HttpServletResponse.class);
		when(response.getOutputStream()).thenReturn(servletOutputStream);

		_classUnderTest.service(request, response);
		servletOutputStream.close();

		return QueryResultIO.parse(
				new ByteArrayInputStream(output.toByteArray()),
				TupleQueryResultFormat.forMIMEType(accept));
	}

	/**
	 * Loads sample data in triple store.
	 * 
	 * @param filename the name of the data file (in the examples set)
	 * @throws Exception in case of failure.
	 */
	protected void loadData(final String filename) throws Exception {
		TRIPLE_STORE.bulkLoad(new File(EXAMPLES_DIR, filename), RDFFormat.TURTLE);
	}

	/**
	 * Loads sample data in triple store.
	 * 
	 * @param filename the name of the data file (in the examples set)
	 * @param format the format of the data.
	 * @throws Exception in case of failure.
	 */
	protected void loadData(final String filename, final RDFFormat format) throws Exception {
		TRIPLE_STORE.bulkLoad(new File(EXAMPLES_DIR, filename), format);
	}

	/**
	 * Reads a query (.rq) file from the example dataset.
	 * 
	 * @param filename the filename.
	 * @return the query as a string.
	 * @throws IOException in case of I/O failure.
	 */
	protected String readQuery(final String filename) throws IOException {
		return FileUtils.readFileToString(new File(EXAMPLES_DIR, filename));
	}

	/**
	 * Shortcut utility for executing query tests.
	 * 
	 * @param dataFile the dataset file.
	 * @param rqFile the query file.
	 * @param expectedBindingSets the expected set of bindings.
	 * @throws Exception in case of query evaluation failure.
	 */
	private void testQuery(final String dataFile, final String rqFile, final List<BindingSet> expectedBindingSets) throws Exception {
		for (final String format : _accept_select_header) {

			final TupleQueryResult result = executeQuery(dataFile, rqFile, format);
			assertTupleResultCorrectness(new ArrayList<BindingSet>(expectedBindingSets), result);
			clean(TRIPLE_STORE);
		}
	}
}