package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.TestUtils.asList;
import static edu.kit.aifb.cumulus.TestUtils.buildLiteral;
import static edu.kit.aifb.cumulus.TestUtils.buildResource;
import static edu.kit.aifb.cumulus.TestUtils.clean;
import static edu.kit.aifb.cumulus.TestUtils.newTripleStore;
import static edu.kit.aifb.cumulus.TestUtils.numOfRes;
import static edu.kit.aifb.cumulus.TestUtils.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.memory.MemoryStore;

import com.google.common.collect.Iterators;

import edu.kit.aifb.cumulus.AbstractCumulusTest;

/** 
 * Test class for the range query functionality of CassandraRdfHectorTriple.
 * Test should be easily adoptable to different datasets.
 * 
 * FIXME: IMPORTANT: rangeResultsTest can fail (wrongly!) if there are 
 * more than one subject that have same datavalue for same property! 
 * e.g. X1 prop 30 X2 prop 30 This is because return order of X1/X2 is 
 * not defined and sesame may return it differently than hector!
 * 
 * @author Felix Obenauer
 * @since 1.0
 */
public class RangeQueriesTest extends AbstractCumulusTest {

	private static RepositoryConnection _repositoryConnection;
	private static final String DATA = "src/test/resources/exp_data.nt";
	
	private final String _subject = "http://izeus1.scc.kit.edu/id/data338", _predicate = "http://gridpedia.org/id/temperature";
	private final double _lowerBound = -10000.75, _upperBound = 130024.75;

	/**
	 * Setup fixture for this test case.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@BeforeClass
	public static void setUp() throws Exception {
		_repository = new SailRepository(new MemoryStore());
		_repository.initialize();

		_tripleStore = newTripleStore();
		_tripleStore.enableRangeIndexesSupport();
		_tripleStore.open();

		assertTrue("Ranges have not been enabled for this triple store!", _tripleStore.isRangeIndexesSupportEnabled());

		_tripleStore.bulkLoad(DATA, RDFFormat.NTRIPLES);

		_repositoryConnection = _repository.getConnection();
		_repositoryConnection.add(new File(DATA), "http://nb-base-uri-not-actually-used", RDFFormat.NTRIPLES);
	}

	/**
	 * Clears the triple store.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@AfterClass
	public static void clear() throws Exception {
		clean(_tripleStore);
		_repositoryConnection.close();
	}

	/**
	 * If an invalid query is received then an empty iterator must be returned.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void invalidQueries() throws Exception {
		final Value[][] invalidQueries = {
				null, // null
				{ null, null, null }, // bad length
				{ buildResource(randomString()) }, // bad length
				{ buildResource(randomString()), null } // p is a variable
		};

		for (final Value[] invalidQuery : invalidQueries) {
			assertEquals(
					"query " + invalidQuery + " must return an empty iterator.",
					0,
					numOfRes(_tripleStore.range(invalidQuery, null, true, null, true, true, Integer.MAX_VALUE)));

			assertSame(
					"query " + invalidQuery + " must return an empty iterator.",
					Iterators.emptyIterator(),
					_tripleStore.rangeAsIDs(invalidQuery, null, true, null, true, true, Integer.MAX_VALUE));
		}
	}

	/**
	 * A literal can be converted in a decimal representation.
	 */
	@Test
	public void boundWithConvertibleLiteral() {
		final String[] numericValues = { "1", "0", "1.9927883", "-0.288382", "132" };
		for (final String value : numericValues) {
			assertEquals(new BigDecimal(value), ((TripleStore) _tripleStore).asDecimal(ValueFactoryImpl.getInstance().createLiteral(value), null));
		}
	}

	/**
	 * A literal cannot be converted in a decimal representation.
	 */
	@Test
	public void boundWithUnconvertibleLiteral() {
		final String[] nAnValues = { null, "", " ", "abc1234def", "ThisIsNaN", };
		for (final String nan : nAnValues) {
			final Literal literal = nan != null ? ValueFactoryImpl.getInstance().createLiteral(nan) : null;
			assertEquals(TripleStore.MAX_UPPER_BOUND, ((TripleStore) _tripleStore).asDecimal(literal, TripleStore.MAX_UPPER_BOUND));
			assertEquals(TripleStore.MIN_LOWER_BOUND, ((TripleStore) _tripleStore).asDecimal(literal, TripleStore.MIN_LOWER_BOUND));
		}
	}

	/**
	 * If the range query has not a match on the store then an empty iterator must be returned.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void noSuchDataInTheStore() throws Exception {
		final URI unmatchingSubject = buildResource(randomString());
		final URI unmatchingPredicate = buildResource(randomString());

		assertEquals(0, numOfRes(_tripleStore.query(new Value[] { unmatchingSubject, null, null })));
		assertEquals(0, numOfRes(_tripleStore.query(new Value[] { unmatchingSubject, unmatchingPredicate, null })));

		final Value[][] invalidQueries = {
				{ unmatchingSubject, unmatchingPredicate },
				{ null, unmatchingPredicate }
		};

		for (final Value[] invalidQuery : invalidQueries) {

			assertEquals(
					"query " + invalidQuery + " must return an empty iterator.",
					0,
					numOfRes(_tripleStore.range(invalidQuery, null, true, null, true, true, Integer.MAX_VALUE)));
		}
	}

	@Test
	public void inverseValueOrderSubjTest() throws CumulusStoreException {

		Value[] query = new Value[2];
		query[0] = buildResource(_subject);
		query[1] = buildResource(_predicate);
		Iterator<Statement> iter = _tripleStore.range(query, null, true, null, true, true, Integer.MAX_VALUE);
		assertTrue(iter != null && iter.hasNext());

		double last = Double.MAX_VALUE;
		double current = Double.MAX_VALUE;
		while (iter.hasNext()) {
			last = current;
			final Statement res = iter.next();
			Literal lit = (Literal) res.getObject();
			current = Double.parseDouble(lit.getLabel());
			assertTrue(last >= current);
		}
	}

	@Test
	public void inverseValueOrderTest() throws CumulusStoreException {
		Value[] query = new Value[2];
		query[0] = null;
		query[1] = buildResource(_predicate);
		Iterator<Statement> iter = _tripleStore.range(query, null, true, null, true, true, Integer.MAX_VALUE);
		assertTrue(iter != null && iter.hasNext());

		double last = Double.MAX_VALUE;
		double current = Double.MAX_VALUE;
		int counter = 0;

		while (iter.hasNext()) {
			last = current;
			Statement res = iter.next();
			Literal lit = (Literal) res.getObject();
			current = Double.parseDouble(lit.getLabel());
			assertTrue(last >= current);
			counter++;
		}

		assertTrue(counter == 320);
	}

	@Test
	public void rangeBoundsSubjTest() throws CumulusStoreException {
		Value[] query = new Value[2];
		query[0] = buildResource(_subject);
		query[1] = buildResource(_predicate);
		double lowerBound = 3.18;
		double upperBound = 3.20;

		for (boolean upper_equals : new boolean[] { true, false }) {

			for (boolean lower_equals : new boolean[] { true, false }) {

				Iterator<Statement> iter = _tripleStore.range(
						query,
						buildLiteral(String.valueOf(lowerBound)),
						lower_equals,
						buildLiteral(String.valueOf(upperBound)),
						upper_equals,
						false,
						Integer.MAX_VALUE);

				final String message = new StringBuilder()
						.append("query = ").append(Arrays.toString(query))
						.append("lower = ").append(lowerBound).append(", ")
						.append("lower_equals = ").append(lower_equals).append(", ")
						.append("upper = ").append(upperBound).append(", ")
						.append("upper_equals = ").append(upper_equals).append(", ")
						.toString();

				assertTrue(message, iter != null && iter.hasNext());

				int counter = 0;
				double current = -Double.MAX_VALUE, last;

				while (iter.hasNext()) {

					last = current;
					Statement res = iter.next();
					Literal lit = (Literal) res.getObject();
					current = Double.parseDouble(lit.getLabel());

					if (upper_equals) {
						assertTrue(current <= upperBound);
					} else {
						assertTrue(current < upperBound);
					}

					if (lower_equals) {
						assertTrue(current >= lowerBound);
					} else {
						assertTrue(current > lowerBound);
					}

					assertTrue(last <= current);
					counter++;
				}

				assertTrue(counter == 1);
			}
		}
	}

	@Test
	public void rangeBoundsTest() throws CumulusStoreException {

		Value[] query = new Value[2];
		query[0] = null;
		query[1] = buildResource(_predicate);
		double lowerBound = 20.33;
		double upperBound = 20.98;

		for (boolean upper_equals : new boolean[] { true, false }) {

			for (boolean lower_equals : new boolean[] { true, false }) {

				Iterator<Statement> iter = _tripleStore.range(query, buildLiteral(String.valueOf(lowerBound)), lower_equals,
						buildLiteral(String.valueOf(upperBound)), upper_equals, false, Integer.MAX_VALUE);
				assertTrue(iter != null && iter.hasNext());

				int counter = 0;
				double current = -Double.MAX_VALUE, last;

				while (iter.hasNext()) {

					last = current;
					Statement res = iter.next();
					Literal lit = (Literal) res.getObject();
					current = Double.parseDouble(lit.getLabel());

					if (upper_equals) {
						assertTrue(current <= upperBound);
					} else {
						assertTrue(current < upperBound);
					}

					if (lower_equals) {
						assertTrue(current >= lowerBound);
					} else {
						assertTrue(current > lowerBound);
					}

					assertTrue(last <= current);
					counter++;
				}

				if (lower_equals) {
					assertTrue(counter == 7);
				} else {
					assertTrue(counter == 6);
				}
			}
		}
	}

	/**
	 * Tests with defined Subject
	 * @throws CumulusStoreException 
	 */

	@Test
	public void rangeResultsSubjTest() throws Exception {

		String SPquery = String.format("SELECT * WHERE {" + "<%s> <%s> ?o ." + "FILTER(?o >= %s && ?o <= %s )" + "}" + "ORDER BY ASC(?o)", _subject,
				_predicate, _lowerBound, _upperBound);

		TupleQuery q = _repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, SPquery);
		TupleQueryResult actres = q.evaluate();

		Value[] query = new Value[2];
		query[0] = buildResource(_subject);
		query[1] = buildResource(_predicate);
		Iterator<Statement> iter = _tripleStore.range(query, buildLiteral(String.valueOf(_lowerBound)), true, buildLiteral(String.valueOf(_upperBound)), true,
				false, Integer.MAX_VALUE);

		assertTrue(iter != null && iter.hasNext());

		while (iter.hasNext()) {
			try {
				Statement testres = iter.next();
				BindingSet compres = actres.next();
				// Compare DataValue - String Comparison fails, since Sesame
				// omits '.0'
				assertEquals(Double.parseDouble(((Literal) testres.getObject()).getLabel()), ((org.openrdf.model.Literal) compres.getValue("o")).doubleValue(),
						1 / 10 ^ 6);
				// Compare Datatype of the Literal
				assertEquals(((Literal) testres.getObject()).getDatatype().toString().replace("<", "").replace(">", ""),
						((org.openrdf.model.Literal) compres.getValue("o")).getDatatype().stringValue());
			} catch (NoSuchElementException ele) {
				fail("Corresponding query via Sesame returned fewer results!");
			}
		}
		if (actres.hasNext()) {
			fail("Corresponding query via Sesame returned more results!");
		}
	}

	/**
	 * Compares range results obtained from SPARQL and Range query.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void rangeResults() throws Exception {
		final String sparql_query = String.format(
				"SELECT * WHERE { ?s <%s> ?o . FILTER(?o >= %s && ?o <= %s ) } ORDER BY ASC(?o)",
				_predicate, _lowerBound, _upperBound);

		final TupleQuery q = _repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, sparql_query);
		final TupleQueryResult sparqlResults = q.evaluate();
		assertTrue(sparqlResults != null && sparqlResults.hasNext());

		final Value[] query = { null, buildResource(_predicate) };

		final Iterator<Statement> rangeResultIterator = _tripleStore.range(
				query,
				buildLiteral(String.valueOf(_lowerBound)),
				true,
				buildLiteral(String.valueOf(_upperBound)),
				true,
				false,
				Integer.MAX_VALUE);

		assertTrue(rangeResultIterator != null && rangeResultIterator.hasNext());
		final List<Statement> rangeResult = asList(rangeResultIterator);

		int howManyBindings = 0;
		int howManyTriples = rangeResult.size();

		while (sparqlResults.hasNext()) {
			howManyBindings++;
			final BindingSet sparqlBindings = sparqlResults.next();

			final Literal objectFromSparqlQuery = (Literal) sparqlBindings.getValue("o");

			for (Iterator<Statement> iterator = rangeResult.iterator(); iterator.hasNext();) {
				final Statement rangeResultTriple = iterator.next();
				final Literal objectFromRangeQuery = (Literal) rangeResultTriple.getObject();

				final BigDecimal valueFromRangeQuery = ((TripleStore) _tripleStore).asDecimal(objectFromRangeQuery, null);
				assertNotNull("Literal object cannot be null for this kind of (range) query result.", valueFromRangeQuery);

				final BigDecimal valueFromSparqlQuery = ((TripleStore) _tripleStore).asDecimal(objectFromSparqlQuery, null);
				assertNotNull("Literal object cannot be null for this kind of (SPARQL) query result.", valueFromSparqlQuery);

				if (isTheSameValue(valueFromRangeQuery.doubleValue(), valueFromSparqlQuery.doubleValue(), 1 / 10 ^ 6)) {
					iterator.remove();
				}
			}
		}

		assertTrue(
				"SPARQL returned " + howManyBindings
						+ " bindings, RangeQuery returned " + howManyTriples
						+ ", remaining triples : "
						+ rangeResult,
				rangeResult.isEmpty());
	}

	/**
	 * Range query with a pattern where both subject and preficate are constants.
	 * 
	 * @throws Exception never, otherwise the tests fail.
	 */
	@Test
	public void rangeQueryWithConstants() throws Exception {
		final Value[] query = { buildResource(_subject), buildResource(_predicate), null };
		final Value[] rangeQuery = { buildResource(_subject), buildResource(_predicate) };

		final Iterator<Statement> queryResultIterator = _tripleStore.query(query);
		final Set<BigDecimal> rangeItems = new TreeSet<BigDecimal>();
		for (; queryResultIterator.hasNext();) {
			final Statement value = queryResultIterator.next();
			final Value object = value.getObject();
			assertNotNull(object);
			if (object instanceof Literal) {
				final BigDecimal numericValue = ((TripleStore) _tripleStore).asDecimal((Literal) object, null);
				assertNotNull(numericValue);
				rangeItems.add(numericValue);
			}
		}

		final Iterator<Statement> iter = _tripleStore.range(rangeQuery, null, true, null, true, false, Integer.MAX_VALUE);
		assertTrue(iter != null && iter.hasNext());

		double last = Double.MIN_VALUE;
		double current = Double.MIN_VALUE;

		final Set<BigDecimal> itemsFromRangeQuery = new TreeSet<BigDecimal>();
		while (iter.hasNext()) {
			last = current;
			final Statement triple = iter.next();
			final Literal literal = (Literal) triple.getObject();
			final BigDecimal numericValue = ((TripleStore) _tripleStore).asDecimal(literal, null);
			assertNotNull(numericValue);
			itemsFromRangeQuery.add(numericValue);
			current = numericValue.doubleValue();
			assertTrue(last <= current);
		}

		assertEquals(rangeItems, itemsFromRangeQuery);
	}

	/**
	 * Range query with a pattern where the subject is a variable.
	 * 
	 * @throws Exception never, otherwise the tests fail.
	 */
	@Test
	public void rangeQueryWithSubjectVariable() throws Exception {
		final Value[] query = { null, buildResource(_predicate), null };
		final Value[] rangeQuery = { null, buildResource(_predicate) };

		final Iterator<Statement> queryResultIterator = _tripleStore.query(query);
		final Set<BigDecimal> rangeItems = new TreeSet<BigDecimal>();
		for (; queryResultIterator.hasNext();) {
			final Statement value = queryResultIterator.next();
			final Value object = value.getObject();
			assertNotNull(object);
			if (object instanceof Literal) {
				final BigDecimal numericValue = ((TripleStore) _tripleStore).asDecimal((Literal) object, null);
				assertNotNull(numericValue);
				rangeItems.add(numericValue);
			}
		}

		assertTrue("At least 2 values need to match the range query.", rangeItems.size() > 1);

		final Iterator<Statement> iter = _tripleStore.range(rangeQuery, null, true, null, true, false, Integer.MAX_VALUE);
		assertTrue(iter != null && iter.hasNext());

		double last = Double.MIN_VALUE;
		double current = Double.MIN_VALUE;

		final Set<BigDecimal> itemsFromRangeQuery = new TreeSet<BigDecimal>();
		while (iter.hasNext()) {
			last = current;
			final Statement triple = iter.next();
			final Literal literal = (Literal) triple.getObject();
			final BigDecimal numericValue = ((TripleStore) _tripleStore).asDecimal(literal, null);
			assertNotNull(numericValue);
			itemsFromRangeQuery.add(numericValue);
			current = numericValue.doubleValue();
			assertTrue(last <= current);
		}

		assertEquals(rangeItems, itemsFromRangeQuery);
	}

	/**
	 * Returns true if two given doubles are equal.
	 * 
	 * @param d1 the first operand.
	 * @param d2 the second operand.
	 * @param delta a delta tolerance.
	 * @return true if two doubles are equal, according with a given tolerance.
	 */
	private boolean isTheSameValue(double d1, double d2, double delta) {
		return (Double.compare(d1, d2) == 0) || ((Math.abs(d1 - d2) <= delta));
	}
}
