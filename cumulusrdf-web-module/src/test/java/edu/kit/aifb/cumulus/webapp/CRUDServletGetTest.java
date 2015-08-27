package edu.kit.aifb.cumulus.webapp;

import static edu.kit.aifb.cumulus.WebTestUtils.asList;
import static edu.kit.aifb.cumulus.WebTestUtils.buildResource;
import static edu.kit.aifb.cumulus.WebTestUtils.numOfRes;
import static edu.kit.aifb.cumulus.WebTestUtils.randomString;
import static edu.kit.aifb.cumulus.WebTestUtils.tmpFile;
import static edu.kit.aifb.cumulus.util.Util.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;

import edu.kit.aifb.cumulus.StubServletOutputStream;
import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Headers;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.MimeTypes;

/**
 * Test cases for the HTTP GET service of the ({@link CRUDServlet}).
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CRUDServletGetTest extends AbstractCRUDServletTest {

	// CHECKSTYLE:OFF
	private String[] _data = {
			"<http://gridpedia.org/id/Device> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#Class> . \n <http://gridpedia.org/id/Device> <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Device> .",
			"<http://gridpedia.org/id/Device> <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" .",
			"<http://gridpedia.org/id/Device> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> . \n <http://gridpedia.org/id/Device> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> ."
	};

	// CHECKSTYLE:ON

	/**
	 * Positive test for HTTP GET on CRUD servlet.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void getData() throws Exception {

		for (final String tripleAsString : _data) {
			
			final Statement triple = parseNX(tripleAsString).iterator().next();
			TRIPLE_STORE.addData(triple);
			
			final Value entity = triple.getSubject();
			final int howManyTriplesDescribingEntity = asList(TRIPLE_STORE.describe(entity, false)).size();
			assertTrue("DESCRIBE for entity " + entity + " cannot have an empty result.", howManyTriplesDescribingEntity > 0);

			for (final String accept : MimeTypes.RDF_SERIALIZATIONS) {

				final File tmp = tmpFile();
				final ServletOutputStream servletOutputStream = new StubServletOutputStream(tmp);

				final HttpServletResponse response = mock(HttpServletResponse.class);				
				when(response.getOutputStream()).thenReturn(servletOutputStream);

				_classUnderTest.doGet(createMockHttpRequest(entity, null, null, null, null, accept), response);
				
				servletOutputStream.close();
				assertEquals("CRUD GET on entity '" + entity + "' and accept '" + accept + "' failed", howManyTriplesDescribingEntity, parseAsList(new FileInputStream(tmp), RDFFormat.forMIMEType(accept)).size());
			}
		}
	}

	/**
	 * If store has no data for a given entity then CRUD service must return 404 status code.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void getDataWithNoSuchEntity() throws Exception {
		
		for (final String tripleAsString : _data) {

			final Statement triple = parseNX(tripleAsString).iterator().next();
			TRIPLE_STORE.removeData(new Value[] { triple.getSubject(), triple.getPredicate(), triple.getObject() });

			int howManyTriplesForPattern = numOfRes(TRIPLE_STORE.query(new Value[] { triple.getSubject(), triple.getPredicate(), triple.getObject() }));
			assertEquals("This triple " + tripleAsString + " mustn't exist on store", 0, howManyTriplesForPattern);

			final HttpServletRequest request = createMockHttpRequest(triple.getSubject(), null, null, null, null);
			final HttpServletResponse response = mock(HttpServletResponse.class);

			_classUnderTest.doGet(request, response);

			verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
		}
	}

	/**
	 * If an internal server error occurs during deletion the service must
	 * answer with 500 HTTP status code. Note that this doesn't cover any
	 * possible scenarios...just emulating an uncaught exception in order to see
	 * if a correct response code is returned.
	 * 
	 * @throws Exception
	 *             hopefully never, otherwise the test fails.
	 */
	@Test
	public void getWithUnknownInternalServerFailure() throws Exception {
		
		when(_context.getAttribute(ConfigParams.SESAME_REPO)).thenReturn(null);

		final HttpServletRequest request = createMockHttpRequest(buildResource(randomString()), null, null, null, null);
		when(request.getHeader(Headers.CONTENT_TYPE)).thenReturn(MimeTypes.TEXT_PLAIN);

		final HttpServletResponse response = mock(HttpServletResponse.class);

		_classUnderTest.doGet(request, response);

		verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	}
}