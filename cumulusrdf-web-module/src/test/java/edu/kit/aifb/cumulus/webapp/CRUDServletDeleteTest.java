package edu.kit.aifb.cumulus.webapp;

import static edu.kit.aifb.cumulus.WebTestUtils.EMPTY_STRINGS;
import static edu.kit.aifb.cumulus.WebTestUtils.SELECT_ALL_TRIPLES_PATTERN;
import static edu.kit.aifb.cumulus.WebTestUtils.buildResource;
import static edu.kit.aifb.cumulus.WebTestUtils.newTripleStore;
import static edu.kit.aifb.cumulus.WebTestUtils.numOfRes;
import static edu.kit.aifb.cumulus.WebTestUtils.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import javax.servlet.RequestDispatcher;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Methods;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Parameters;

/**
 * Test case for Delete REST service ({@link CRUDServlet}) on a triple store.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CRUDServletDeleteTest extends AbstractCRUDServletTest {

	private String _aThing = "http://gridpedia.org/id/Thing";
	
	private String[] _unmatchingUri = {
			"http://gridpedia.org/id/Thing2", 
			"http://gridpedia.org/id/Actor2",
			"http://www.w3.org/2000/01/rdf-schema#Class2"};

	private String[] _deleteUri = {
			_aThing,
			"http://www.w3.org/2000/01/rdf-schema#Class",
			"http://gridpedia.org/id/Actor"};

	private Value[][] _deletePatterns = {
		new Value[] {null, null, buildResource(_aThing) },
		new Value[] {null, RDF.TYPE, null },
		new Value[] {buildResource("http://gridpedia.org/id/Actor"), RDFS.SEEALSO, buildResource("http://dbpedia.org/resource/Actant") } };

	private Value[][] _unmatchingPatterns = {
		new Value[] {null, null, buildResource(randomString()) },
		new Value[] {null, buildResource(randomString()), buildResource(randomString())},
		new Value[] {null, buildResource(randomString()), null }, new Value[] {buildResource(randomString()), null, null}};
	
	/**
	 * If a "uri" parameter is specified {@link CRUDServlet} will delete the
	 * corresponding entity. On top of receiving a valid URI parameter it will
	 * use a DESCRIBE to delete the whole entity description (i.e. triples where
	 * that URI is subject or object).
	 * 
	 * @throws Exception
	 *             hopefully never, otherwise the test fails.
	 */
	@Test
	public void deleteEntity() throws Exception {

		for (final String uriAsString : _deleteUri) {

			final URI uri = buildResource(uriAsString);
			final int howManyTriplesBeforeDeleting = numOfRes(TRIPLE_STORE.describe(uri, false));

			assertTrue("There must be at least 1 triple describing entity " + uri, howManyTriplesBeforeDeleting > 0);

			_classUnderTest.doDelete(createMockHttpRequest(uri, null, null, null, null), mock(HttpServletResponse.class));

			final int howManyTriplesAfterDeleting = numOfRes(TRIPLE_STORE.describe(uri, false));
			assertEquals("Entity has not (completely) deleted.", 0, howManyTriplesAfterDeleting);
		}
	}

	/**
	 * If a "uri" parameter is specified {@link CRUDServlet} will delete the
	 * corresponding entity. On top of receiving a valid URI parameter it will
	 * use a DESCRIBE to delete the whole entity description (i.e. triples where
	 * that URI is subject or object).
	 * 
	 * If there are no matching triples that describe the given entity then
	 * delete won't have any effect on the store.
	 * 
	 * @throws Exception
	 *             hopefully never, otherwise the test fails.
	 */
	@Test
	public void deleteEntityWithNoMatchingData() throws Exception {

		for (final String uriAsString : _unmatchingUri) {

			final URI uri = buildResource(uriAsString);
			final int howManyTriplesForThatEntityBeforeDeleting = numOfRes(TRIPLE_STORE.describe(uri, false));
			final int howManyTriplesOnStoreBeforeDeleting = numOfRes(TRIPLE_STORE.query(SELECT_ALL_TRIPLES_PATTERN));

			assertEquals("There must be no triple matching for entity " + uriAsString, 0, howManyTriplesForThatEntityBeforeDeleting);

			assertTrue("At least one triple must exist and it mustn't match DESCRIBE " + uriAsString, howManyTriplesOnStoreBeforeDeleting > 0);

			final HttpServletResponse response = mock(HttpServletResponse.class);
			final HttpServletRequest request = createMockHttpRequest(uri, null, null, null, null);

			_classUnderTest.doDelete(request, response);

			verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);

			final int howManyTriplesOnStoreAfterDeleting = numOfRes(TRIPLE_STORE.query(SELECT_ALL_TRIPLES_PATTERN));

			assertEquals(howManyTriplesOnStoreBeforeDeleting, howManyTriplesOnStoreAfterDeleting);
		}
	}

	/**
	 * A delete command is not considered valid if all input parameters are
	 * null. The service will return BAD_REQUEST http status code and the store
	 * won't have any changes.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void deleteWithInvalidInput() throws Exception {

		final int howManyTriplesOnStoreBeforeDeleting = numOfRes(TRIPLE_STORE.query(SELECT_ALL_TRIPLES_PATTERN));
		assertTrue("At least one triple must exists on the store.", howManyTriplesOnStoreBeforeDeleting > 0);

		internalDeleteWithInvalidInput(null, null, null, null, null);

		for (final String invalidParameter : EMPTY_STRINGS) {
			internalDeleteWithInvalidInput(invalidParameter, null, null, null, null);
			internalDeleteWithInvalidInput(null, invalidParameter, null, null, null);
			internalDeleteWithInvalidInput(null, null, invalidParameter, null, null);
			internalDeleteWithInvalidInput(null, null, null, invalidParameter, null);
			internalDeleteWithInvalidInput(null, null, null, null, invalidParameter);
		}

		final int howManyTriplesOnStoreAfterDeleting = numOfRes(TRIPLE_STORE.query(SELECT_ALL_TRIPLES_PATTERN));

		assertEquals(howManyTriplesOnStoreBeforeDeleting, howManyTriplesOnStoreAfterDeleting);
	}

	/**
	 * {@link CRUDServlet} must be able to delete by pattern. In this case the
	 * uri parameter must be null (otherwise a deletionByEntity will be
	 * executed) and system will compose a deletion pattern by using s,p,o,c
	 * parameters.
	 * 
	 * @throws Exception
	 *             hopefully never, otherwise the test fails.
	 */
	@Test
	public void deleteByPattern() throws Exception {

		for (final Value[] pattern : _deletePatterns) {

			final int howManyTriplesBeforeDeleting = numOfRes(TRIPLE_STORE.query(pattern));
			assertTrue("A minimum of 1 matching triple is needed for pattern " + Arrays.toString(pattern), howManyTriplesBeforeDeleting > 0);

			_classUnderTest.doDelete(createMockHttpRequest(null, pattern[0], pattern[1], pattern[2], null), mock(HttpServletResponse.class));

			final int howManyTriplesAfterDeleting = numOfRes(TRIPLE_STORE.query(pattern));
			assertEquals("Delete not executed or executed partially.", 0, howManyTriplesAfterDeleting);
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
	public void deleteWithUnknownInternalServerFailure() throws Exception {
		when(_context.getAttribute(ConfigParams.STORE)).thenReturn(null);

		final HttpServletRequest request = createMockHttpRequest(null, buildResource(randomString()), null, null, null);
		final HttpServletResponse response = mock(HttpServletResponse.class);

		_classUnderTest.doDelete(request, response);

		verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	}

	/**
	 * If CumulusStoreExceptionm occurs during deletion the service must
	 * answer with 500 HTTP status code. Note that this doesn't cover any
	 * possible scenarios...it just uses an uninitialized store in order to see
	 * if a correct response code is returned.
	 * 
	 * @throws Exception
	 *             hopefully never, otherwise the test fails.
	 */
	@Test
	public void deleteWithCumulusInternalServerFailure() throws Exception {
		when(_context.getAttribute(ConfigParams.STORE)).thenReturn(newTripleStore());

		final HttpServletRequest request = createMockHttpRequest(null, buildResource(randomString()), null, null, null);
		final HttpServletResponse response = mock(HttpServletResponse.class);

		_classUnderTest.doDelete(request, response);

		verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	}

	/**
	 * If given pattern doesn't match any triple on the store the delete command
	 * won't have any effect.
	 * 
	 * @throws Exception
	 *             hopefully never, otherwise the test fails.
	 */
	@Test
	public void deleteByPatternWithNoMatchingData() throws Exception {

		for (final Value[] pattern : _unmatchingPatterns) {

			final int howManyTriplesForThatPatternBeforeDeleting = numOfRes(TRIPLE_STORE.query(pattern));
			final int howManyTriplesOnStoreBeforeDeleting = numOfRes(TRIPLE_STORE.query(SELECT_ALL_TRIPLES_PATTERN));

			assertEquals("There must be no triple matching pattern " + Arrays.toString(pattern), 0, howManyTriplesForThatPatternBeforeDeleting);

			assertTrue("At least one triple must exist and it mustn't match pattern " + Arrays.toString(pattern),
					howManyTriplesOnStoreBeforeDeleting > 0);

			final HttpServletResponse response = mock(HttpServletResponse.class);
			final HttpServletRequest request = createMockHttpRequest(null, pattern[0], pattern[1], pattern[2], null);

			_classUnderTest.doDelete(request, response);

			verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);

			final int howManyTriplesOnStoreAfterDeleting = numOfRes(TRIPLE_STORE.query(SELECT_ALL_TRIPLES_PATTERN));

			assertEquals(howManyTriplesOnStoreBeforeDeleting, howManyTriplesOnStoreAfterDeleting);
		}
	}

	/**
	 * Internal method used for testing several invalid input scenarios.
	 * 
	 * @param uri the URI (delete by entity).
	 * @param s the subject.
	 * @param p the predicate.
	 * @param o the object.
	 * @param c the context.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	private void internalDeleteWithInvalidInput(
			final String uri, 
			final String s, 
			final String p, 
			final String o, 
			final String c) throws Exception {

		final HttpServletResponse response = mock(HttpServletResponse.class);
		final HttpServletRequest request = mock(HttpServletRequest.class);
		when(request.getParameter(Parameters.URI)).thenReturn(uri != null ? uri : null);
		when(request.getParameter(Parameters.S)).thenReturn(s == null ? null : String.valueOf(s));
		when(request.getParameter(Parameters.P)).thenReturn(p == null ? null : String.valueOf(p));
		when(request.getParameter(Parameters.O)).thenReturn(o == null ? null : String.valueOf(o));
		when(request.getParameter(Parameters.C)).thenReturn(c == null ? null : String.valueOf(c));
		when(request.getRequestDispatcher(anyString())).thenReturn(mock(RequestDispatcher.class));
		when(request.getMethod()).thenReturn(Methods.POST);
		
		_classUnderTest.doDelete(request, response);

		verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
	}
}