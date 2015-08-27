package edu.kit.aifb.cumulus.webapp;

import static edu.kit.aifb.cumulus.WebTestUtils.clean;
import static edu.kit.aifb.cumulus.WebTestUtils.newTripleStore;
import static edu.kit.aifb.cumulus.WebTestUtils.tmpFile;
import static edu.kit.aifb.cumulus.util.Util.parseAsList;
import static edu.kit.aifb.cumulus.util.Util.parseNX;
import static edu.kit.aifb.cumulus.webapp.writer.HTMLWriter.HTML_FORMAT;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.rio.Rio;

import edu.kit.aifb.cumulus.AbstractCumulusWebTest;
import edu.kit.aifb.cumulus.StubServletOutputStream;
import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.framework.Environment.ConfigValues;
import edu.kit.aifb.cumulus.store.CumulusStoreException;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFSail;
import edu.kit.aifb.cumulus.util.Util;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Headers;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Methods;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.MimeTypes;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Parameters;
import edu.kit.aifb.cumulus.webapp.writer.HTMLWriter;

public class LinkedDataServletTest extends AbstractCumulusWebTest {

	private HttpServletRequest _request;
	private HttpServletResponse _response;
	private LinkedDataServlet _ld_servlet;

	private static List<String> URIS, URIS_INVALID, TRIPLES_NT, TRIPLES_INVALID_NT, TRIPLES_INVALID_XML;

	static {

		URIS = Arrays.asList("http://gridpedia.org/id/Actor");
		URIS_INVALID = Arrays.asList("http://gridpedia.org/id/Actor2");

		TRIPLES_INVALID_XML = Arrays.asList("<?xml version=\"1.0\" encoding=\"utf-8\"?> <rdf:RDF x");

		TRIPLES_NT = Arrays
				.asList("<http://gridpedia.org/id/Actor> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#Class> .\n "
						+ "<http://gridpedia.org/id/Actor> <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Actor> .\n <http://gridpedia.org/id/Actor>"
						+ " <http://semantic-mediawiki.org/swivt/1.0#wikiPageModificationDate> \"2012-02-01T09:53:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n "
						+ "<http://gridpedia.org/id/Actor> <http://www.w3.org/2000/01/rdf-schema#comment> \"Actors have the capability to make decisions and exchange "
						+ "information with other actors through interfaces. Actors my be devices, computer systems, or software programs and/or the organizations that own them. "
						+ "An actor may also comprise other actors. Source: NIST Framework and Roadmap for Smart Grid Interoperability Standards, National Institute of Standards"
						+ " and Technology (2010).\"^^<http://www.w3.org/2001/XMLSchema#string> .\n <http://gridpedia.org/id/Actor> <http://www.w3.org/2000/01/rdf-schema#comment> "
						+ "\"Netznutzer: nat\u00FCrliche oder juristische Personen, die Energie in ein Elektrizit\u00E4ts- oder Gasversorgungsnetz einspeisen oder daraus beziehen "
						+ "(http://bundesrecht.juris.de/enwg_2005/__3.html). Quelle: MeRegioMobil http://meregiomobil.forschung.kit.edu/.\"^^<http://www.w3.org/2001/XMLSchema#string> .\n"
						+ " <http://gridpedia.org/id/Actor> <http://www.w3.org/2000/01/rdf-schema#comment> \"Ein Agent ist eine (nat\u00FCrliche oder juristische) Person,"
						+ " welche eine Transaktion auf dem Markt im Auftrag seines Kunden oder seines Arbeitgebers ausf\u00FChrt. Es kann erforderlich sein, dass f\u00FCr gewisse "
						+ "Transaktionen ein Personenbezug hergestellt werden kann. Regelungen des Wertpapierhandelsgesetztes sind gegebenenfalls zu ber\u00FCcksichtigen, da der "
						+ "Marktplatz b\u00F6rsen\u00E4hnlich aufgebaut ist.Quelle: MeRegioMobil http://meregiomobil.forschung.kit.edu/.\"^^<http://www.w3.org/2001/XMLSchema#string> .\n"
						+ " <http://gridpedia.org/id/Actor> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <http://gridpedia.org/wiki/Actor> .\n <http://gridpedia.org/id/Actor> "
						+ "<http://www.w3.org/2000/01/rdf-schema#label> \"Actor\"^^<http://www.w3.org/2001/XMLSchema#string> .\n <http://gridpedia.org/id/Actor> <http://www.w3.org/2000/01/rdf-schema#seeAlso> "
						+ "<http://dbpedia.org/resource/Actant> .\n <http://gridpedia.org/id/Actor> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Thing> .\n");

		TRIPLES_INVALID_NT = Arrays.asList("<http://gridpedia.org/id/Actor> <http://www.w3.org/1999/02/22-rd");

		/*
		 * ensure that our Sesame HTMLWriter is registered
		 */
		RDFFormat.register(HTML_FORMAT);
		RDFWriterRegistry.getInstance().add(new RDFWriterFactory() {

			@Override
			public RDFFormat getRDFFormat() {
				return HTML_FORMAT;
			}

			@Override
			public RDFWriter getWriter(OutputStream out) {
				return new HTMLWriter(out);
			}

			@Override
			public RDFWriter getWriter(Writer writer) {
				return new HTMLWriter(writer);
			}
		});
	}

	@BeforeClass
	public static void setUp() throws Exception {
		TRIPLE_STORE = newTripleStore();
		SAIL = new CumulusRDFSail(TRIPLE_STORE);
		REPOSITORY = new SailRepository(SAIL);
		REPOSITORY.initialize();
	}

	/**
	 * Tear down fixture for each test.
	 * Basically deletes all data in the store.
	 * 
	 * @throws Exception hopefully never, otherwise the corresponding test fails.
	 */
	@After
	public void afterEachTest() throws Exception {
		clean(TRIPLE_STORE);
	}

	@Before
	public void beforeEachTest() throws Exception {

		ServletContext context = mock(ServletContext.class);
		when(context.getAttribute(ConfigParams.SESAME_REPO)).thenReturn(REPOSITORY);
		when(context.getAttribute(ConfigParams.STORE)).thenReturn(TRIPLE_STORE);
		when(context.getAttribute(ConfigParams.LAYOUT)).thenReturn(ConfigValues.STORE_LAYOUT_TRIPLE);

		_ld_servlet = spy(new LinkedDataServlet());
		_ld_servlet.init();

		doReturn(context).when(_ld_servlet).getServletContext();
		when(context.getNamedDispatcher(anyString())).thenReturn(mock(RequestDispatcher.class));

		_request = mock(HttpServletRequest.class);
		when(_request.getRequestDispatcher(anyString())).thenReturn(mock(RequestDispatcher.class));
		when(_request.getHeader(Headers.CONTENT_TYPE)).thenReturn(MimeTypes.N_TRIPLES);
		when(_request.getMethod()).thenReturn(Methods.POST);

		_response = mock(HttpServletResponse.class);

		TRIPLE_STORE.bulkLoad(DATA_NT, RDFFormat.NTRIPLES);

		for (int i = 0; i < TRIPLES_NT.size(); i++) {

			final String triples = TRIPLES_NT.get(i);
			when(_request.getInputStream()).thenReturn(new ServletInputStream() {

				private InputStream _delegate = new ByteArrayInputStream(triples.getBytes());

				@Override
				public int read() throws IOException {
					return _delegate.read();
				}
			});

			_ld_servlet.doPost(_request, _response);
		}
	}

	@Test
	public void delete() throws IOException, CumulusStoreException, ServletException {

		for (int i = 0; i < URIS.size(); i++) {

			/*
			 * prepare mock ...
			 */
			String uri = URIS.get(i);
			when(_request.getRequestURL()).thenReturn(new StringBuffer(uri));

			/*
			 * delete entity
			 */
			_ld_servlet.doDelete(_request, _response);

			/*
			 * verify deletion
			 */
			verify(_response).setStatus(HttpServletResponse.SC_OK);
			assertTrue("entity '" + uri + "' should have been deleted", !TRIPLE_STORE.query(new Value[] { ValueFactoryImpl.getInstance().createURI(uri), null, null })
					.hasNext());
		}
	}

	@Test
	public void deleteInvalid() throws IOException, ServletException {

		for (int i = 0; i < URIS_INVALID.size(); i++) {

			/*
			 * prepare mock ...
			 */
			when(_request.getRequestURL()).thenReturn(new StringBuffer(URIS_INVALID.get(i)));

			/*
			 * delete entity
			 */
			_ld_servlet.doDelete(_request, _response);

			/*
			 * verify deletion
			 */
			verify(_response).setStatus(HttpServletResponse.SC_NOT_FOUND);
		}
	}

	@Test
	public void get() throws IOException, Exception {

		for (String mime_type : MimeTypes.RDF_SERIALIZATIONS) {

			when(_request.getHeader(Parameters.ACCEPT)).thenReturn(mime_type);

			for (int i = 0; i < URIS.size(); i++) {

				/*
				 * prepare mock ...
				 */
				File tmp = tmpFile();
				when(_response.getOutputStream()).thenReturn(new StubServletOutputStream(tmp));
				when(_request.getRequestURL()).thenReturn(new StringBuffer(URIS.get(i)));

				/*
				 * get data
				 */
				_ld_servlet.doGet(_request, _response);

				/*
				 * verify ...
				 */
				verify(_response, atLeastOnce()).setStatus(HttpServletResponse.SC_OK);
				assertTrue(parseAsList(new FileInputStream(tmp), RDFFormat.forMIMEType(mime_type)).size() > 0);
			}
		}
	}

	@Test
	public void getHTML() throws IOException, Exception {

		when(_request.getHeader(Parameters.ACCEPT)).thenReturn(MimeTypes.TEXT_HTML);

		for (int i = 0; i < URIS.size(); i++) {

			/*
			 * prepare mock ...
			 */
			when(_response.getOutputStream()).thenReturn(new StubServletOutputStream(tmpFile()));
			when(_request.getRequestURL()).thenReturn(new StringBuffer(URIS.get(i)));

			/*
			 * get data
			 */
			_ld_servlet.doGet(_request, _response);

			/*
			 * verify ...
			 */
			verify(_response, atLeastOnce()).setStatus(HttpServletResponse.SC_OK);
		}
	}

	@Test
	public void getInvalid() throws IOException, ServletException {

		for (String mime_type : MimeTypes.RDF_SERIALIZATIONS) {

			when(_request.getHeader(Parameters.ACCEPT)).thenReturn(mime_type);

			for (int i = 0; i < URIS_INVALID.size(); i++) {

				/*
				 * prepare mock ...
				 */
				when(_request.getRequestURL()).thenReturn(new StringBuffer(URIS_INVALID.get(i)));

				/*
				 * get data
				 */
				_ld_servlet.doGet(_request, _response);

				/*
				 * verify ...
				 */
				verify(_response, atLeastOnce()).setStatus(HttpServletResponse.SC_NOT_FOUND);
			}
		}
	}

	@Test
	public void post() throws IOException, CumulusStoreException, RDFHandlerException, ServletException {

		for (int i = 0; i < TRIPLES_NT.size(); i++) {

			for (String mime_type : MimeTypes.RDF_SERIALIZATIONS) {

				/*
				 * clear data ...
				 */
				TRIPLE_STORE.clear();
				assertTrue("store should be empty", !TRIPLE_STORE.query(new Value[] { null, null, null }).hasNext());

				/*
				 * prepare data in desired RDF serialization
				 */

				Model model = new LinkedHashModel(parseNX(TRIPLES_NT.get(i)));
				final ByteArrayOutputStream out = new ByteArrayOutputStream();
				Rio.write(model, out, RDFFormat.forMIMEType(mime_type));

				/*
				 * prepare mock ...
				 */
				when(_request.getHeader(Headers.CONTENT_TYPE)).thenReturn(mime_type);
				when(_request.getInputStream()).thenReturn(new ServletInputStream() {
					final InputStream _inputStream = new ByteArrayInputStream(out.toByteArray());

					@Override
					public int read() throws IOException {
						return _inputStream.read();
					}
				});

				/*
				 * HTTP POST 
				 */
				_ld_servlet.doPost(_request, _response);

				/*
				 * verify the HTTP POST ...
				 */
				verify(_response, atLeastOnce()).setStatus(HttpServletResponse.SC_CREATED);

				for (Statement stmt : model) {
					assertTrue("statement '" + stmt + "' has not been been added correctly for serialization '" + mime_type + "'", TRIPLE_STORE.query(Util.toValueArray(stmt))
							.hasNext());
				}
			}
		}
	}

	@Test
	public void postInvalidNT() throws IOException, ServletException {

		when(_request.getHeader(Parameters.CONTENT_TYPE)).thenReturn(MimeTypes.N_TRIPLES);

		for (int i = 0; i < TRIPLES_INVALID_NT.size(); i++) {

			/*
			 * prepare mock ...
			 */
			final String triples = TRIPLES_INVALID_NT.get(i);
			when(_request.getInputStream()).thenReturn(new ServletInputStream() {

				private InputStream _delegate = new ByteArrayInputStream(triples.getBytes());

				@Override
				public int read() throws IOException {
					return _delegate.read();
				}
			});

			/*
			 * HTTP POST ...
			 */
			_ld_servlet.doPost(_request, _response);

			/*
			 * verify the HTTP POST ...
			 */
			verify(_response, atLeastOnce()).setStatus(HttpServletResponse.SC_BAD_REQUEST);
		}
	}

	@Test
	public void postInvalidXML() throws IOException, URISyntaxException, ServletException {

		when(_request.getHeader(Parameters.CONTENT_TYPE)).thenReturn(MimeTypes.RDF_XML);

		for (int i = 0; i < TRIPLES_INVALID_XML.size(); i++) {

			/*
			 * prepare mock ...
			 */
			final String triples = TRIPLES_INVALID_XML.get(i);
			when(_request.getInputStream()).thenReturn(new ServletInputStream() {

				private InputStream _delegate = new ByteArrayInputStream(triples.getBytes());

				@Override
				public int read() throws IOException {
					return _delegate.read();
				}
			});

			/*
			 * HTTP POST ...
			 */
			_ld_servlet.doPost(_request, _response);

			/*
			 * verify the HTTP POST ...
			 */
			verify(_response, atLeastOnce()).setStatus(HttpServletResponse.SC_BAD_REQUEST);
		}
	}
}