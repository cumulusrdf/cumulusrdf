package edu.kit.aifb.cumulus.webapp;

import static edu.kit.aifb.cumulus.WebTestUtils.EMPTY_STRINGS;
import static edu.kit.aifb.cumulus.WebTestUtils.buildLiteral;
import static edu.kit.aifb.cumulus.WebTestUtils.buildResource;
import static edu.kit.aifb.cumulus.WebTestUtils.newTripleStore;
import static edu.kit.aifb.cumulus.WebTestUtils.numOfRes;
import static edu.kit.aifb.cumulus.WebTestUtils.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.ntriples.NTriplesUtil;

import edu.kit.aifb.cumulus.framework.Environment;
import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Headers;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Methods;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.MimeTypes;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Parameters;

/**
 * Test case for Put REST service ({@link CRUDServlet}) on a triple store.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CRUDServletPutTest extends AbstractCRUDServletTest {
	/**
	 * Creates a mock {@link HttpServletRequest} with given params.
	 * 
	 * @param s the subject.
	 * @param p the predicate.
	 * @param o the object.
	 * @param c the context.
	 * @param s2 the new subject.
	 * @param p2 the new predicate.
	 * @param o2 the new object.
	 * @param c2 the new context.
	 * @return a mock {@link HttpServletRequest}.
	 */
	protected HttpServletRequest createMockHttpRequest(
			final Value s, final Value p, final Value o, final Value c,
			final Value s2, final Value p2, final Value o2, final Value c2) {

		final HttpServletRequest request = super.createMockHttpRequest(null, s, p, o, c);

		when(request.getParameter(Parameters.S2)).thenReturn(s2 == null ? null : NTriplesUtil.toNTriplesString(s2));
		when(request.getParameter(Parameters.P2)).thenReturn(p2 == null ? null : NTriplesUtil.toNTriplesString(p2));
		when(request.getParameter(Parameters.O2)).thenReturn(o2 == null ? null : NTriplesUtil.toNTriplesString(o2));
		when(request.getParameter(Parameters.C2)).thenReturn(c2 == null ? null : NTriplesUtil.toNTriplesString(c2));
		when(request.getRequestDispatcher(anyString())).thenReturn(mock(RequestDispatcher.class));
		return request;
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
	public void putWithCumulusInternalServerFailure() throws Exception {
		when(_context.getAttribute(ConfigParams.STORE)).thenReturn(newTripleStore());
		
		final URI uri = buildResource(randomString());
		final HttpServletRequest request = createMockHttpRequest(uri, null, null, null, uri, null, null, null);
		when(request.getHeader(Headers.CONTENT_TYPE)).thenReturn(MimeTypes.TEXT_PLAIN);
	
		final HttpServletResponse response = mock(HttpServletResponse.class);
		
		_classUnderTest.doPut(request, response);

		verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	}
	
	/**
	 * The old triple must contain at least one constant.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void putWithInvalidInput() throws Exception {
		final HttpServletResponse response = mock(HttpServletResponse.class);
		final HttpServletRequest request = createMockHttpRequest(null, null, null, null, null, null, null, null);
		final ServletInputStream stream = new ServletInputStream() {
			final InputStream _inputStream = new ByteArrayInputStream(EMPTY_STRINGS[0].getBytes(Environment.CHARSET_UTF8));
			
			@Override
			public int read() throws IOException {
				return _inputStream.read();
			}
		};
		
		when(request.getInputStream()).thenReturn(stream);
		
		
		_classUnderTest.doPut(request, response);

		verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
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
	public void putWithInvalidUri() throws Exception {
		final String invalidUri = randomString();
		 
		final HttpServletRequest request = mock(HttpServletRequest.class);
		when(request.getParameter(Parameters.S)).thenReturn(invalidUri);
		when(request.getHeader(Headers.CONTENT_TYPE)).thenReturn(MimeTypes.TEXT_PLAIN);
		when(request.getRequestDispatcher(anyString())).thenReturn(mock(RequestDispatcher.class));
		when(request.getMethod()).thenReturn(Methods.POST);
		
		final HttpServletResponse response = mock(HttpServletResponse.class);

		_classUnderTest.doPut(request, response);

		verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	}		
	
	/**
	 * If the old triple has only constants the update will affect only that triple.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void putWithOneMatchingTriple() throws Exception {
		
		final URI subject = buildResource("http://gridpedia.org/id/Actor");
		final URI predicate = buildResource("http://www.w3.org/2000/01/rdf-schema#label");
		final Literal object = buildLiteral("Actor");
		final Literal newObject = buildLiteral("Attore");
		
		final Value[] pattern = new Value[]{subject, predicate, object};
		final Value[] expectedTriple = new Value[]{subject, predicate, newObject};
		
		assertEquals(1, numOfRes(TRIPLE_STORE.query(pattern)));
		
		final HttpServletResponse response = mock(HttpServletResponse.class);
		final HttpServletRequest request = createMockHttpRequest(subject, predicate, object, null, null, null, newObject, null);
		
		_classUnderTest.doPut(request, response);

		assertEquals(1, numOfRes(TRIPLE_STORE.query(expectedTriple)));
		assertEquals(0, numOfRes(TRIPLE_STORE.query(pattern)));
	}	

	/**
	 * If the old triple is a constant and it doesn't match anything then the new triple will be applied.
	 * 
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void putWithOneUnmatchingTriple() throws Exception {
		final URI unexistentSubject = buildResource(randomString());
		final URI unexistentPredicate = buildResource(randomString());
		final Literal unexistentObject = buildLiteral(randomString());
		
		final Literal unexistentNewObject = buildLiteral(randomString());
		
		final Value[] pattern = new Value[]{unexistentSubject, unexistentPredicate, unexistentObject};
		assertEquals(0, numOfRes(TRIPLE_STORE.query(pattern)));
		
		final HttpServletResponse response = mock(HttpServletResponse.class);
		final HttpServletRequest request = createMockHttpRequest(
				unexistentSubject, unexistentPredicate, unexistentObject, 
				null, null, null, unexistentNewObject, null);
		
		_classUnderTest.doPut(request, response);

		assertEquals(0, numOfRes(TRIPLE_STORE.query(pattern)));
		assertEquals(1, numOfRes(TRIPLE_STORE.query(new Value[]{unexistentSubject, unexistentPredicate, unexistentNewObject})));
	}	
	
	
	/**
	 * If the old triple is a constant and it doesn't match anything then a new triple will be created.
	 * In this first scenario the new triple pattern is not valorized...so that means the old (unexistent triple) 
	 * will be created.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void putWithOneUnmatchingTripleAndNoNewTriple() throws Exception {
		final URI unexistentSubject = buildResource(randomString());
		final URI unexistentPredicate = buildResource(randomString());
		final Literal unexistentObject = buildLiteral(randomString());
		
		final Value[] pattern = new Value[]{unexistentSubject, unexistentPredicate, unexistentObject};
		assertEquals(0, numOfRes(TRIPLE_STORE.query(pattern)));
		
		final HttpServletResponse response = mock(HttpServletResponse.class);
		final HttpServletRequest request = createMockHttpRequest(
				unexistentSubject, unexistentPredicate, unexistentObject, 
				null, null, null, null, null);
		
		_classUnderTest.doPut(request, response);

		assertEquals(1, numOfRes(TRIPLE_STORE.query(pattern)));
	}		
	
	/**
	 * If the old triple is a pattern and it doesn't match anything then a 404 (NOT_FOUND) code must be returned.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void putWithPatternAndNoMatchingData() throws Exception {
		final URI unexistentUri = buildResource(randomString());
		
		assertEquals(0, numOfRes(TRIPLE_STORE.describe(unexistentUri, false)));
		
		final HttpServletResponse response = mock(HttpServletResponse.class);
		final HttpServletRequest request = createMockHttpRequest(unexistentUri, null, null, null, null, null, null, null);
		
		_classUnderTest.doPut(request, response);

		verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
	}		
	
	/**
	 * If old triple is a pattern then the first matching triple will be updated.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void putWithSeveralMatchingTriples() throws Exception {
		final URI subject = buildResource("http://gridpedia.org/id/Actor");
		final Literal newObject = buildLiteral("This is the value that will be set for the first matching Actor triple.");
		
		final Value[] pattern = new Value[]{subject, null, null};
		int howManyTriplesForActorAsSubject = numOfRes(TRIPLE_STORE.query(pattern));
		
		assertTrue(howManyTriplesForActorAsSubject > 0);
		
		final HttpServletResponse response = mock(HttpServletResponse.class);
		final HttpServletRequest request = createMockHttpRequest(subject, null, null, null, null, null, newObject, null);
		
		_classUnderTest.doPut(request, response);
		
		int howManyTriplesFoundAfterUpdating = 0;
		boolean oneTripleHasBeenUpdated = false; 
		for (final Iterator<Statement> iterator = TRIPLE_STORE.query(pattern); iterator.hasNext();) {
			final Statement triple = iterator.next();
			if (newObject.equals(triple.getObject()))
			{
				if (oneTripleHasBeenUpdated)
				{
					fail("Another triple has already been updated.");
				} else
				{
					oneTripleHasBeenUpdated = true;					
				}
			}
			howManyTriplesFoundAfterUpdating++;
		}

		assertTrue(oneTripleHasBeenUpdated);
		assertEquals(howManyTriplesFoundAfterUpdating, howManyTriplesForActorAsSubject);
	}	
}