package edu.kit.aifb.cumulus.webapp;

import static edu.kit.aifb.cumulus.WebTestUtils.clean;
import static edu.kit.aifb.cumulus.WebTestUtils.newTripleStore;
import static edu.kit.aifb.cumulus.WebTestUtils.numOfRes;
import static edu.kit.aifb.cumulus.util.Util.parseNX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.Vector;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.methods.GetMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.http.client.BackgroundTupleResult;
import org.openrdf.http.protocol.Protocol;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.query.resultio.TupleQueryResultParserRegistry;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.StatementCollector;

import edu.kit.aifb.cumulus.AbstractCumulusWebTest;
import edu.kit.aifb.cumulus.StubServletOutputStream;
import edu.kit.aifb.cumulus.WebTestUtils;
import edu.kit.aifb.cumulus.framework.Environment;
import edu.kit.aifb.cumulus.store.RepositoryManager;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFSail;
import edu.kit.aifb.cumulus.webapp.endpoint.SesameHTTPProtocolEndpoint;

/**
 * Test the SesameHTTPRepositoryAdapter for the use of HttpRepository.
 * 
 * @author Yongtao
 *
 */
public class SesameHTTPRepositoryTest extends AbstractCumulusWebTest {

	protected SesameHTTPProtocolEndpoint _classUnderTest;
	private static ValueFactory valueFactory = ValueFactoryImpl.getInstance();

	private static final String DEVICE = "<http://gridpedia.org/id/Device>";

	public static final String DATA_NT = "src/test/resources/triples_gridpedia.nt",
			DATA_NT_LOCAL = "src/test/resources/triples_gridpedia_localhost.nt",
			DATA_RDF_XML = "src/test/resources/triples_gridpedia.rdf",
			DATA_NQ = "src/test/resources/quads.nq";

	/** BASE_URI **/
	private static final String BASE_URI = "/cumulusrdf-web-module/";

	/** HTTP method "GET".*/
	private static final String METHOD_GET = "GET";

	/** HTTP method "POST". */
	private static final String METHOD_POST = "POST";

	/** HTTP method "PUT". */
	private static final String METHOD_PUT = "PUT";

	private static final String MIMETYPE_TXT_N3 = "text/n3";
	private static final String MIMETYPE_RDF_XML = "application/rdf+xml";
	private static final String MIMETYPE_WWW_FORM = "application/x-www-form-urlencoded";

	private static final String UTF_8 = "UTF-8";

	protected ServletContext _context;

	private static final String REPO_ID = "repo";

	//CHECKSTYLE:OFF
	private String[] _data = {
			"<http://gridpedia.org/id/Device> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#Class> . \n <http://gridpedia.org/id/Device> <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Device> .",
			"<http://gridpedia.org/id/Device> <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" .",
			"<http://gridpedia.org/id/Device> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> . \n <http://gridpedia.org/id/Device> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> ."
	};

	//CHECKSTYLE:ON


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

		RepositoryManager.getInstance().addRepository(REPO_ID, REPOSITORY, true);
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
	 * Setup fixture for each test.
	 * Initializes mock stuff and load sample data.
	 * 
	 * @throws Exception hopefully never, otherwise the corresponding test fails.
	 */
	@Before
	public void beforeEachTest() throws Exception {
		_context = mock(ServletContext.class);

		_classUnderTest = spy(new SesameHTTPProtocolEndpoint());
		doReturn(_context).when(_classUnderTest).getServletContext();
		when(_context.getNamedDispatcher(anyString())).thenReturn(mock(RequestDispatcher.class));
	}

	/**
	 * mock the HttpServletRequest.
	 * 
	 * @param uri the uri
	 * @param method the method e.g. "GET" and "POST"
	 * @return the mocked HttpServletRequest object
	 */
	private static HttpServletRequest mockHttpRequest(final String uri, final String method) {
		final HttpServletRequest request = mock(HttpServletRequest.class);
		when(request.getRequestURI()).thenReturn(uri);
		when(request.getMethod()).thenReturn(method);
		when(request.getRequestDispatcher(anyString())).thenReturn(mock(RequestDispatcher.class));
		return request;
	}

	/**
	 * test getting the protocol Version.
	 * @throws Exception the test would fails if exception occurs
	 */
	@Test
	public void testVersion() throws Exception {
		File tmp = WebTestUtils.tmpFile();
		final ServletOutputStream servletOutputStream = new StubServletOutputStream(tmp);
		final HttpServletRequest request = mockHttpRequest(BASE_URI + Protocol.PROTOCOL, METHOD_GET);
		final HttpServletResponse response = mock(HttpServletResponse.class);
		when(response.getOutputStream()).thenReturn(servletOutputStream);

		_classUnderTest.service(request, response);

		BufferedReader br = new BufferedReader(new FileReader(tmp));
		String version = br.readLine();
		br.close();
		assertTrue(version.equals(Protocol.VERSION));
		servletOutputStream.close();
	}

	/**
	 * test statements related operations.
	 * @throws Exception Exception the test would fails if exception occurs
	 */
	@Test
	public void testStatements() throws Exception {
		//test add Statements
		for (final String tripleAsString : _data) {

			final Statement triple = parseNX(tripleAsString).iterator().next();
			final Value[] query = new Value[] { triple.getSubject(), triple.getPredicate(), triple.getObject() };
			TRIPLE_STORE.removeData(query);

			//ensure the repository contains not statements to be added
			int howManyTriplesForPattern = numOfRes(TRIPLE_STORE.query(query));
			assertEquals("This triple " + tripleAsString + " mustn't exist on store", 0, howManyTriplesForPattern);

			String uri = BASE_URI + Protocol.REPOSITORIES + "/" + REPO_ID + "/" + Protocol.STATEMENTS;
			final ServletInputStream stream = new ServletInputStream() {
				final InputStream _inputStream = new ByteArrayInputStream(tripleAsString.getBytes(Environment.CHARSET_UTF8));

				@Override
				public int read() throws IOException {
					return _inputStream.read();
				}
			};
			final HttpServletRequest request = mockHttpRequest(uri, METHOD_PUT);
			when(request.getInputStream()).thenReturn(stream);
			when(request.getContentType()).thenReturn(MIMETYPE_TXT_N3);
			when(request.getCharacterEncoding()).thenReturn(UTF_8);
			File tmp = WebTestUtils.tmpFile();
			final ServletOutputStream servletOutputStream = new StubServletOutputStream(tmp);
			final HttpServletResponse response = mock(HttpServletResponse.class);
			when(response.getOutputStream()).thenReturn(servletOutputStream);

			_classUnderTest.service(request, response);

			verify(response).setStatus(HttpServletResponse.SC_NO_CONTENT);

			int howManyTriplesOnTheStore = numOfRes(TRIPLE_STORE.query(query));
			assertEquals(1, howManyTriplesOnTheStore);
		}
	}

	/**
	 * test get Statemetns.
	 * @throws Exception The test would fails if Exception occurs
	 */
	@Test
	public void testGetStatements() throws Exception {
		//insert some statments first
		for (final String tripleAsString : _data) {
			final Statement triple = parseNX(tripleAsString).iterator().next();
			TRIPLE_STORE.addData(triple);
		}
		//test get the statements
		String uri = BASE_URI + Protocol.REPOSITORIES + "/" + REPO_ID + "/" + Protocol.STATEMENTS;
		File tmp = WebTestUtils.tmpFile();
		final ServletOutputStream servletOutputStream = new StubServletOutputStream(tmp);
		final HttpServletRequest request = mockHttpRequest(uri, METHOD_GET);
		when(request.getParameter(Protocol.SUBJECT_PARAM_NAME)).thenReturn(DEVICE);
		when(request.getParameter(Protocol.ACCEPT_PARAM_NAME)).thenReturn(MIMETYPE_RDF_XML);
		when(request.getCharacterEncoding()).thenReturn(UTF_8);
		final HttpServletResponse response = mock(HttpServletResponse.class);
		when(response.getOutputStream()).thenReturn(servletOutputStream);

		GetMethod method = new GetMethod(uri);
		RDFFormat format = RDFFormat.RDFXML;
		RDFParser parser = Rio.createParser(format, valueFactory);
		parser.setParserConfig(parser.getParserConfig());
		StatementCollector collector = new StatementCollector();
		parser.setRDFHandler(collector);
		_classUnderTest.service(request, response);

		parser.parse(new FileInputStream(tmp), method.getURI().getURI());
		assertTrue(!collector.getStatements().isEmpty());
		verify(response).setStatus(HttpServletResponse.SC_OK);

		servletOutputStream.close();
	}

	/**
	 * test get Graph Query.
	 * @throws Exception The test would fails if Exception occurs
	 */
	@Test
	public void testGraphQuery() throws Exception {
		//insert some statments first
		for (final String tripleAsString : _data) {
			final Statement triple = parseNX(tripleAsString).iterator().next();
			TRIPLE_STORE.addData(triple);
		}
		//test Graph Query
		String queryString = "SELECT ?x ?y WHERE { ?x ?p ?y } ";
		String uri = BASE_URI + Protocol.REPOSITORIES + "/" + REPO_ID;
		File tmp = WebTestUtils.tmpFile();
		final ServletOutputStream servletOutputStream = new StubServletOutputStream(tmp);
		final HttpServletRequest request = mockHttpRequest(uri, METHOD_POST);
		when(request.getContentType()).thenReturn(MIMETYPE_WWW_FORM);
		when(request.getParameter(Protocol.QUERY_LANGUAGE_PARAM_NAME)).thenReturn(QueryLanguage.SPARQL.toString());
		when(request.getParameter(Protocol.QUERY_PARAM_NAME)).thenReturn(queryString);
		when(request.getParameter(Protocol.ACCEPT_PARAM_NAME)).thenReturn("application/x-binary-rdf-results-table");
		Vector<String> vec = new Vector<String>();
		when(request.getParameterNames()).thenReturn(vec.elements());
		when(request.getCharacterEncoding()).thenReturn(UTF_8);
		final HttpServletResponse response = mock(HttpServletResponse.class);
		when(response.getOutputStream()).thenReturn(servletOutputStream);


		_classUnderTest.service(request, response);
		
		Set<TupleQueryResultFormat> tqrFormats = TupleQueryResultParserRegistry.getInstance().getKeys();
		TupleQueryResultFormat format = TupleQueryResultFormat.matchMIMEType("application/x-binary-rdf-results-table", tqrFormats);
		TupleQueryResultParser parser = QueryResultIO.createParser(format, valueFactory);
		FileInputStream in = new FileInputStream(tmp);
		BackgroundTupleResult tRes = new BackgroundTupleResult(parser, in, null);
		parser.setQueryResultHandler(tRes);
		parser.parseQueryResult(in);
		assertTrue(tRes.hasNext());
		verify(response).setStatus(HttpServletResponse.SC_OK);

		servletOutputStream.close();
	}

}
