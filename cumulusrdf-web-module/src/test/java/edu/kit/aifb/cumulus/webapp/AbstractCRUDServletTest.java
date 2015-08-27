package edu.kit.aifb.cumulus.webapp;

import static edu.kit.aifb.cumulus.WebTestUtils.SELECT_ALL_TRIPLES_PATTERN;
import static edu.kit.aifb.cumulus.WebTestUtils.clean;
import static edu.kit.aifb.cumulus.WebTestUtils.newTripleStore;
import static edu.kit.aifb.cumulus.WebTestUtils.numOfRes;
import static edu.kit.aifb.cumulus.util.Util.parseNX;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.ntriples.NTriplesUtil;

import edu.kit.aifb.cumulus.AbstractCumulusWebTest;
import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.framework.Environment.ConfigValues;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFSail;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Headers;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Methods;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Parameters;

/**
 * Supertype layer for all {@link CRUDServlet} tests.
 * 
 * @author Andrea Gazzarini
 * @since 1.1
 */
public abstract class AbstractCRUDServletTest extends AbstractCumulusWebTest {
	
	/**
	 * Prepares the execution of this test case.
	 * 
	 * @throws Exception hopefully never, otherwise the whole test case fails.
	 */
	@BeforeClass
	public static void beforeAllTests() throws Exception {
		TRIPLE_STORE = newTripleStore();
		SAIL = new CumulusRDFSail(TRIPLE_STORE);
		REPOSITORY = new SailRepository(SAIL);
		REPOSITORY.initialize();
	}

	protected final String _actor = "<http://gridpedia.org/id/Actor>";

	//CHECKSTYLE:OFF 
	protected final String _triplesAsString =
			"<http://gridpedia.org/id/Actor> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#Class> .\n "
					+ "<http://gridpedia.org/id/Actor> <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Actor> .\n "
					+ "<http://gridpedia.org/id/Actor> <http://semantic-mediawiki.org/swivt/1.0#wikiPageModificationDate> \"2012-02-01T09:53:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n "

					+ "<http://gridpedia.org/id/Actor> <http://www.w3.org/2000/01/rdf-schema#comment> \"Actors have the capability to make decisions and exchange "
					+ "information with other actors through interfaces. Actors my be devices, computer systems, or software programs and/or the organizations that own them. "
					+ "An actor may also comprise other actors. Source: NIST Framework and Roadmap for Smart Grid Interoperability Standards, National Institute of Standards"
					+ " and Technology (2010).\"^^<http://www.w3.org/2001/XMLSchema#string> .\n "
					+ "<http://gridpedia.org/id/Actor> <http://www.w3.org/2000/01/rdf-schema#comment> \"Netznutzer: nat\u00FCrliche oder juristische Personen, die Energie in ein Elektrizit\u00E4ts- oder Gasversorgungsnetz einspeisen oder daraus beziehen "
					+ "(http://bundesrecht.juris.de/enwg_2005/__3.html). Quelle: MeRegioMobil http://meregiomobil.forschung.kit.edu/.\"^^<http://www.w3.org/2001/XMLSchema#string> .\n "

					+ "<http://gridpedia.org/id/Actor> <http://www.w3.org/2000/01/rdf-schema#comment> \"Ein Agent ist eine (nat\u00FCrliche oder juristische) Person,"
					+ " welche eine Transaktion auf dem Markt im Auftrag seines Kunden oder seines Arbeitgebers ausf\u00FChrt. Es kann erforderlich sein, dass f\u00FCr gewisse "
					+ "Transaktionen ein Personenbezug hergestellt werden kann. Regelungen des Wertpapierhandelsgesetztes sind gegebenenfalls zu ber\u00FCcksichtigen, da der "
					+ "Marktplatz b\u00F6rsen\u00E4hnlich aufgebaut ist.Quelle: MeRegioMobil http://meregiomobil.forschung.kit.edu/.\"^^<http://www.w3.org/2001/XMLSchema#string> .\n "

					+ "<http://gridpedia.org/id/Actor> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <http://gridpedia.org/wiki/Actor> .\n "
					+ "<http://gridpedia.org/id/Actor> <http://www.w3.org/2000/01/rdf-schema#label> \"Actor\" .\n "
					+ "<http://gridpedia.org/id/Actor> <http://www.w3.org/2000/01/rdf-schema#seeAlso> <http://dbpedia.org/resource/Actant> .\n "
					+ "<http://gridpedia.org/id/Actor> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Thing> .\n";
	//CHECKSTYLE:ON 
	protected final List<Statement> _triples = parseNX(_triplesAsString);

	protected final int _testSetSize = numOfRes(_triples.iterator());
	protected ServletContext _context;

	protected CRUDServlet _classUnderTest;

	/**
	 * Teardown fixture for each test.
	 * 
	 * @throws Exception hopefully never, otherwise the corresponding test fails.
	 */
	@After
	public void afterEachTest() throws Exception {
		clean(TRIPLE_STORE);
	}

	/**
	 * Setup fixture for each test.
	 * 
	 * @throws Exception
	 *             hopefully never, otherwise the corresponding test fails.
	 */
	@Before
	public void beforeEachTest() throws Exception {
		_context = mock(ServletContext.class);
		when(_context.getAttribute(ConfigParams.SESAME_REPO)).thenReturn(REPOSITORY);
		when(_context.getAttribute(ConfigParams.STORE)).thenReturn(TRIPLE_STORE);
		when(_context.getAttribute(ConfigParams.LAYOUT)).thenReturn(ConfigValues.STORE_LAYOUT_TRIPLE);

		_classUnderTest = spy(new CRUDServlet());
		_classUnderTest.init();

		doReturn(_context).when(_classUnderTest).getServletContext();
		doReturn(_context).when(_classUnderTest).getServletContext();
		
		when(_context.getNamedDispatcher(anyString())).thenReturn(mock(RequestDispatcher.class));
		TRIPLE_STORE.addData(_triples.iterator());

		assertEquals(
				"Testset hasn't been properly load on triplestore.",
				_testSetSize,
				numOfRes(TRIPLE_STORE.query(SELECT_ALL_TRIPLES_PATTERN)));
	}

	/**
	 * Creates a mock {@link HttpServletRequest} with given params.
	 * 
	 * @param uri the entity URI (used for describe).
	 * @param s the subject.
	 * @param p the predicate.
	 * @param o the object.
	 * @param c the context.
	 * @return a mock {@link HttpServletRequest}.
	 */
	protected HttpServletRequest createMockHttpRequest(final Value uri, final Value s, final Value p, final Value o, final Value c) {
		return createMockHttpRequest(uri, s, p, o, c, null);
	}

	/**
	 * Creates a mock {@link HttpServletRequest} with given params.
	 * 
	 * @param uri the entity URI (used for describe).
	 * @param s the subject.
	 * @param p the predicate.
	 * @param o the object.
	 * @param c the context.
	 * @param MIME type in accept header.
	 * @return a mock {@link HttpServletRequest}.
	 */
	protected HttpServletRequest createMockHttpRequest(final Value uri, final Value s, final Value p, final Value o, final Value c, String accept_header) {

		final HttpServletRequest request = mock(HttpServletRequest.class);

		when(request.getParameter(Parameters.URI)).thenReturn(uri == null ? null : NTriplesUtil.toNTriplesString(uri));
		when(request.getParameter(Parameters.S)).thenReturn(s == null ? null : NTriplesUtil.toNTriplesString(s));
		when(request.getParameter(Parameters.P)).thenReturn(p == null ? null : NTriplesUtil.toNTriplesString(p));
		when(request.getParameter(Parameters.O)).thenReturn(o == null ? null : NTriplesUtil.toNTriplesString(o));
		when(request.getParameter(Parameters.C)).thenReturn(c == null ? null : NTriplesUtil.toNTriplesString(c));
		when(request.getRequestDispatcher(anyString())).thenReturn(mock(RequestDispatcher.class));
		when(request.getMethod()).thenReturn(Methods.POST);

		if (accept_header != null) {
			when(request.getHeader(Headers.ACCEPT)).thenReturn(accept_header);
		}

		return request;
	}
}