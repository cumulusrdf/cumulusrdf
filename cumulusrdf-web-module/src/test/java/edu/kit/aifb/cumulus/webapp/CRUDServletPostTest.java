package edu.kit.aifb.cumulus.webapp;

import static edu.kit.aifb.cumulus.WebTestUtils.newTripleStore;
import static edu.kit.aifb.cumulus.WebTestUtils.numOfRes;
import static edu.kit.aifb.cumulus.WebTestUtils.randomString;
import static edu.kit.aifb.cumulus.util.Util.parseNX;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;

import edu.kit.aifb.cumulus.framework.Environment;
import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.util.Util;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Headers;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.MimeTypes;

/**
 * Test case for Post REST service ({@link CRUDServlet}) over a CumulusRDF triple store.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CRUDServletPostTest extends AbstractCRUDServletTest {

	//CHECKSTYLE:OFF
	private String[] _data = {
			"<http://gridpedia.org/id/Device> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#Class> . \n <http://gridpedia.org/id/Device> <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Device> .",
			"<http://gridpedia.org/id/Device> <http://www.w3.org/2000/01/rdf-schema#label> \"Device\"^^<http://www.w3.org/2001/XMLSchema#string> .",
			"<http://gridpedia.org/id/Device> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> . \n <http://gridpedia.org/id/Device> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> ."
	};

	//CHECKSTYLE:ON

	/**
	 * Positive test for HTTP Post on the ({@link CRUDServlet}).
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void postData() throws Exception {

		for (String tripleAsString : _data) {

			for (String mime_type : MimeTypes.RDF_SERIALIZATIONS) {

				Model model = new LinkedHashModel(parseNX(tripleAsString));

				/*
				 * clean data
				 */
				TRIPLE_STORE.removeData(model.iterator());
				
				for (Statement statement : model) {					
					assertEquals("This triple " + tripleAsString + " mustn't exist in the store", 0, numOfRes(TRIPLE_STORE.query(Util.toValueArray(statement))));
				}
				
				/*
				 * convert to desired RDF serialization
				 */
				final ByteArrayOutputStream out = new ByteArrayOutputStream();
				Rio.write(model, out, RDFFormat.forMIMEType(mime_type));

				/*
				 * prepare mock ...
				 */
				final HttpServletRequest request = createMockHttpRequest(null, null, null, null, null);
				when(request.getHeader(Headers.CONTENT_TYPE)).thenReturn(mime_type);

				final HttpServletResponse response = mock(HttpServletResponse.class);
				when(request.getInputStream()).thenReturn(new ServletInputStream() {
					final InputStream _inputStream = new ByteArrayInputStream(out.toByteArray());

					@Override
					public int read() throws IOException {
						return _inputStream.read();
					}
				});

				/*
				 * POST data ...
				 */
				_classUnderTest.doPost(request, response);

				/*
				 * verify HTTP POST
				 */
				verify(response).setStatus(HttpServletResponse.SC_CREATED);
				
				for (Statement statement : model) {					
					assertEquals("HTTP POST failed for content-type: '" + mime_type + "' and statement: '" + statement + "'", 1, numOfRes(TRIPLE_STORE.query(Util.toValueArray(statement))));
				}
			}
		}
	}

	/**
	 * If CumulusStoreException occurs during deletion the service must
	 * answer with 500 HTTP status code. Note that this doesn't cover any
	 * possible scenarios...it just uses an uninitialized store in order to see
	 * if a correct response code is returned.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void postWithCumulusInternalServerFailure() throws Exception {

		when(_context.getAttribute(ConfigParams.STORE)).thenReturn(newTripleStore());

		final HttpServletRequest request = createMockHttpRequest(null, null, null, null, null);
		when(request.getHeader(Headers.CONTENT_TYPE)).thenReturn(MimeTypes.TEXT_PLAIN);

		final ServletInputStream stream = new ServletInputStream() {
			final InputStream _inputStream = new ByteArrayInputStream(_triplesAsString.getBytes(Environment.CHARSET_UTF8));

			@Override
			public int read() throws IOException {
				return _inputStream.read();
			}
		};

		when(request.getInputStream()).thenReturn(stream);
		final HttpServletResponse response = mock(HttpServletResponse.class);

		_classUnderTest.doPost(request, response);

		verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		verify(request).setAttribute("javax.servlet.error.status_code", HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	}

	/**
	 * If request body contains malformed data then an appropriate (400 BAD_REQUEST) response code must be returned.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 *             
	 */
	@Test
	public void postWithMalformedData() throws Exception {

		final HttpServletRequest request = createMockHttpRequest(null, null, null, null, null);

		for (String content_type : MimeTypes.RDF_SERIALIZATIONS) {

			when(request.getHeader(Headers.CONTENT_TYPE)).thenReturn(content_type);

			final ServletInputStream stream = new ServletInputStream() {
				final InputStream _inputStream = new ByteArrayInputStream(randomString().getBytes(Environment.CHARSET_UTF8));

				@Override
				public int read() throws IOException {
					return _inputStream.read();
				}
			};

			when(request.getInputStream()).thenReturn(stream);
			final HttpServletResponse response = mock(HttpServletResponse.class);

			_classUnderTest.doPost(request, response);

			verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
		}
	}

	/**
	 * If an internal server error occurs during deletion the service must
	 * answer with 500 HTTP status code. Note that this doesn't cover any
	 * possible scenarios...just emulating an uncaught exception in order to see
	 * if a correct response code is returned.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void postWithUnknownInternalServerFailure() throws Exception {

		when(_context.getAttribute(ConfigParams.STORE)).thenReturn(null);

		final HttpServletRequest request = createMockHttpRequest(null, null, null, null, null);
		when(request.getHeader(Headers.CONTENT_TYPE)).thenReturn(MimeTypes.TEXT_PLAIN);

		final ServletInputStream stream = new ServletInputStream() {
			final InputStream _inputStream = new ByteArrayInputStream(_triplesAsString.getBytes(Environment.CHARSET_UTF8));

			@Override
			public int read() throws IOException {
				return _inputStream.read();
			}
		};

		when(request.getInputStream()).thenReturn(stream);
		final HttpServletResponse response = mock(HttpServletResponse.class);

		_classUnderTest.doPost(request, response);

		verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	}
}