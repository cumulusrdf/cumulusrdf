package edu.kit.aifb.cumulus.webapp;

import static edu.kit.aifb.cumulus.WebTestUtils.EMPTY_STRINGS;
import static edu.kit.aifb.cumulus.WebTestUtils.clean;
import static edu.kit.aifb.cumulus.WebTestUtils.newTripleStore;
import static edu.kit.aifb.cumulus.WebTestUtils.tmpFile;
import static edu.kit.aifb.cumulus.util.Util.*;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;

import edu.kit.aifb.cumulus.AbstractCumulusWebTest;
import edu.kit.aifb.cumulus.StubServletOutputStream;
import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.framework.Environment.ConfigValues;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFSail;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Methods;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.MimeTypes;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Parameters;

/**
 * Test cases for the {@link SPARQLServlet}.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SPARQLServletTest extends AbstractCumulusWebTest {

	/**
	 * Setup fixture for all tests.
	 * 
	 * @throws Exception never otherwise tests fail.
	 */
	@BeforeClass
	public static void setUp() throws Exception {
		TRIPLE_STORE = newTripleStore();
		SAIL = new CumulusRDFSail(TRIPLE_STORE);
		REPOSITORY = new SailRepository(SAIL);
		REPOSITORY.initialize();
	}

	protected SPARQLServlet _classUnderTest;

	private final String[] _invalid_queries = {
			"ASK { ?d <http://www.w3.org/2000/01/rdf-schema#",
			"CONSTRUCT ASK { ?d <http://www.w3.org/2000/01/rdf-schema#",
			"SELECT * WHERE {"
	};

	private final String[] _accept_ask_header = {
			BooleanQueryResultFormat.TEXT.getDefaultMIMEType(),
			BooleanQueryResultFormat.JSON.getDefaultMIMEType(),
			BooleanQueryResultFormat.SPARQL.getDefaultMIMEType()
	};

	private final String[] _accept_select_header = {
			TupleQueryResultFormat.BINARY.getDefaultMIMEType(),
			TupleQueryResultFormat.TSV.getDefaultMIMEType(),
			TupleQueryResultFormat.JSON.getDefaultMIMEType(),
			TupleQueryResultFormat.SPARQL.getDefaultMIMEType(),
			TupleQueryResultFormat.CSV.getDefaultMIMEType()
	};

	protected ServletContext _context;

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

		TRIPLE_STORE.bulkLoad(DATA_NT, RDFFormat.NTRIPLES);
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
		when(request.getParameter(Parameters.ACCEPT)).thenReturn(acceptParameter);
		when(request.getHeader(Parameters.ACCEPT)).thenReturn(acceptHeader);
		when(request.getParameter(Parameters.UPDATE)).thenReturn(update);
		when(request.getRequestDispatcher(anyString())).thenReturn(mock(RequestDispatcher.class));
		when(request.getMethod()).thenReturn(Methods.POST);
		
		return request;
	}

	/**
	 * ASK queries with several supported formats.
	 * 
	 * @param query the ASK queries.
	 * @param acceptAsParameter accept as request parameter.
	 * @param acceptAsHeader accept as request header.
	 * @throws Exception never, otherwise test fails.
	 */
	private void internalValidAskQueries(
			final String query,
			final String acceptAsParameter,
			final String acceptAsHeader) throws Exception {

		final String accept = acceptAsParameter != null ? acceptAsParameter : acceptAsHeader;
		final File tmp = tmpFile();
		final ServletOutputStream servletOutputStream = new StubServletOutputStream(tmp);

		final HttpServletRequest request = createMockHttpRequest(query, acceptAsParameter, acceptAsHeader, null);
		final HttpServletResponse response = mock(HttpServletResponse.class);
		when(response.getOutputStream()).thenReturn(servletOutputStream);

		_classUnderTest.service(request, response);
		servletOutputStream.close();

		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(tmp);
			assertTrue(
					"query: " + query + ", accept: " + accept,
					QueryResultIO.parse(inputStream, BooleanQueryResultFormat.forMIMEType(accept)));
		} finally {
			// CHECKSTYLE:OFF
			// @formatter:off
			if (inputStream != null) { try { inputStream.close();} catch (final Exception ignore) {}};
			// @formatter:on
			// CHECKSTYLE:ON					
		}
	}

	/**
	 * If an invalid SPARQL query is received, then a 400 (BAD_REQUEST) should be returned.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void invalidQueries() throws Exception {

		for (final String query : _invalid_queries) {

			final HttpServletRequest request = createMockHttpRequest(
					query,
					RDFFormat.RDFXML.getDefaultFileExtension(),
					null,
					null);
			final HttpServletResponse response = mock(HttpServletResponse.class);

			_classUnderTest.service(request, response);

			verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
		}
	}

	/**
	 * If an input params are null, then a 400 (BAD_REQUEST) should be returned.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void nullInputParameters() throws Exception {

		for (final String invalidQuery : EMPTY_STRINGS) {
			nullInputParameters(invalidQuery, RDFFormat.RDFXML.getDefaultFileExtension(), null, null);
			nullInputParameters(null, RDFFormat.RDFXML.getDefaultFileExtension(), null, invalidQuery);
		}
	}

	/**
	 * Internal method for testing several invalid input scenarios.
	 * 
	 * @param query the (invalid) query.
	 * @param update the (invalid) update query.
	 * @param acceptAsParam the (invalid) accept request parameter.
	 * @param acceptAsHeader the (invalid) accept header.
	 * @throws Exception never, otherwise the test fails.
	 */
	private void nullInputParameters(
			final String query,
			final String acceptAsParam,
			final String acceptAsHeader,
			final String update) throws Exception {

		final HttpServletRequest request = createMockHttpRequest(query, acceptAsParam, acceptAsHeader, update);
		final HttpServletResponse response = mock(HttpServletResponse.class);

		_classUnderTest.service(request, response);

		verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
	}

	/**
	 * ASK queries with several supported formats (sending accept as request header).
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void validAskQueriesWithAcceptHeader() throws Exception {

		for (final String query : _sparql_ask) {
			for (final String accept : _accept_ask_header) {
				internalValidAskQueries(query, null, accept);
			}
		}
	}

	/**
	 * ASK queries with several supported formats (sending accept as request parameter).
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void validAskQueriesWithAcceptParam() throws Exception {

		for (final String query : _sparql_ask) {
			for (final String accept : _accept_ask_header) {
				internalValidAskQueries(query, accept, null);
			}
		}
	}

	/**
	 * CONSTRUCT queries with several supported formats.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void validConstructQueriesWithAcceptParam() throws Exception {

		for (final String query : _sparql_construct) {

			for (final String accept : MimeTypes.RDF_SERIALIZATIONS) {

				final File tmp = tmpFile();
				final ServletOutputStream servletOutputStream = new StubServletOutputStream(tmp);

				final HttpServletRequest request = createMockHttpRequest(query, null, accept, null);
				final HttpServletResponse response = mock(HttpServletResponse.class);
				when(response.getOutputStream()).thenReturn(servletOutputStream);

				_classUnderTest.service(request, response);
				servletOutputStream.close();

				assertTrue("query: " + query + ", accept: " + accept, parseAsIterator(new FileInputStream(tmp), RDFFormat.forMIMEType(accept)).hasNext());
			}
		}
	}

	/**
	 * SELECT queries with several supported formats.
	 * 
	 * @throws Exception never, otherwise test fails.
	 */
	@Test
	public void validSelectQueriesWithAcceptParam() throws Exception {

		for (final String query : _sparql_select) {

			for (final String accept : _accept_select_header) {

				final File tmp = tmpFile();
				final ServletOutputStream servletOutputStream = new StubServletOutputStream(tmp);

				final HttpServletRequest request = mock(HttpServletRequest.class);
				when(request.getParameter(Parameters.QUERY)).thenReturn(query);
				when(request.getParameter(Parameters.ACCEPT)).thenReturn(accept);
				when(request.getMethod()).thenReturn(Methods.POST);

				final HttpServletResponse response = mock(HttpServletResponse.class);
				when(response.getOutputStream()).thenReturn(servletOutputStream);

				_classUnderTest.service(request, response);
				servletOutputStream.close();

				final TupleQueryResult res = QueryResultIO.parse(new FileInputStream(tmp), TupleQueryResultFormat.forMIMEType(accept));
				assertTrue("query: " + query + ", accept: " + accept, _queries2count.get(query) > 0 ? res.hasNext() : !res.hasNext());
			}
		}
	}
}