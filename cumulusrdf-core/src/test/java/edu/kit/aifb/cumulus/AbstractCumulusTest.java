package edu.kit.aifb.cumulus;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.openrdf.model.Statement;
import org.openrdf.repository.Repository;

import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFSail;

/**
 * Supertype layer for all CumulusRDF test cases.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class AbstractCumulusTest implements TestData {

	public static final String DATA_NT = "src/test/resources/triples_gridpedia.nt",
			DATA_NQ = "src/test/resources/quads.nq";

	public static final List<String> SPARQL_QUERIES = new LinkedList<String>();
	public static final List<String> SPARQL_ASK = new LinkedList<String>();
	public static final List<String> SPARQL_SELECT = new LinkedList<String>();
	public static final List<String> SPARQL_CONSTRUCT = new LinkedList<String>();
	public static final Map<String, Integer> QUERIES_2_COUNT = new HashMap<String, Integer>();

	static {

		SPARQL_ASK.add("ASK { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . }");
		SPARQL_ASK.add("ASK { ?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> . }");
		SPARQL_ASK.add(
				"ASK { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . "
						+ "?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> "
						+ "<http://gridpedia.org/id/Actor> . }");
		SPARQL_ASK.add("ASK { <http://gridpedia.org/id/Media> <http://semantic-mediawiki.org/swivt/1.0#page> ?d . }");
		SPARQL_QUERIES.addAll(SPARQL_ASK);

		SPARQL_SELECT.add("SELECT * WHERE { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . "
				+ "?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> . }");
		QUERIES_2_COUNT.put(SPARQL_SELECT.get(0), 1);

		SPARQL_SELECT.add(
				"SELECT * WHERE { ?a <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Actor> . "
						+ "?a <http://www.w3.org/2000/01/rdf-schema#label> ?l . ?a ?p ?o . }");
		QUERIES_2_COUNT.put(SPARQL_SELECT.get(1), 10);

		SPARQL_SELECT.add(
				"SELECT * WHERE { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . ?d "
						+ "<http://www.w3.org/2000/01/rdf-schema#subClassOf> ?a . ?a <http://semantic-mediawiki.org/swivt/1.0#page> "
						+ "<http://gridpedia.org/wiki/Actor> .}");
		QUERIES_2_COUNT.put(SPARQL_SELECT.get(2), 1);

		SPARQL_SELECT.add(
				"SELECT * WHERE { ?d <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Media> . "
						+ "?d <http://semantic-mediawiki.org/swivt/1.0#wikiPageModificationDate> ?date . "
						+ "FILTER ( ?date >= \"2012-01-30T00:00:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ) } LIMIT 1 ");
		QUERIES_2_COUNT.put(SPARQL_SELECT.get(3), 1);

		SPARQL_SELECT.add("SELECT * WHERE { ?d <http://semantic-mediawiki.org/swivt/1.0#page> ?p . } ORDER BY DESC(?p) ");
		QUERIES_2_COUNT.put(SPARQL_SELECT.get(4), 42);

		SPARQL_SELECT.add("SELECT DISTINCT ?d WHERE { ?d <http://semantic-mediawiki.org/swivt/1.0#page> ?p . } ");
		QUERIES_2_COUNT.put(SPARQL_SELECT.get(5), 42);

		SPARQL_SELECT.add("SELECT * WHERE { ?d <http://semantic-mediawiki.org/swivt/1.0#page> ?p . } ORDER BY DESC(?p) LIMIT 1 ");
		QUERIES_2_COUNT.put(SPARQL_SELECT.get(6), 1);

		SPARQL_SELECT.add(
				"SELECT * WHERE { ?d <http://semantic-mediawiki.org/swivt/1.0#page> ?p . "
						+ "OPTIONAL { ?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> } }");
		QUERIES_2_COUNT.put(SPARQL_SELECT.get(7), 42);

		SPARQL_SELECT.add(
				"SELECT * WHERE { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device2\" . "
						+ "?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> . }");
		QUERIES_2_COUNT.put(SPARQL_SELECT.get(8), 0);

		SPARQL_SELECT.add("SELECT ?d WHERE { ?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor2> . }");
		QUERIES_2_COUNT.put(SPARQL_SELECT.get(9), 0);

		SPARQL_QUERIES.addAll(SPARQL_SELECT);

		SPARQL_CONSTRUCT
				.add("CONSTRUCT { ?d <http://xmlns.com/foaf/0.1/name> \"Device\" } { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . }");
		QUERIES_2_COUNT.put(SPARQL_CONSTRUCT.get(0), 1);

		SPARQL_CONSTRUCT.add(
				"CONSTRUCT { ?d <http://xmlns.com/foaf/0.1/name> \"Device\" } "
						+ "{ ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . ?d <http://www.w3.org/2002/07/owl#sameAs> "
						+ "<http://dbpedia.org/resource/Device> . }");
		QUERIES_2_COUNT.put(SPARQL_CONSTRUCT.get(1), 1);

		SPARQL_CONSTRUCT.add(
				"CONSTRUCT { ?d <http://www.w3.org/2000/01/rdf-schema#seeAlso> ?a } "
						+ "{ ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . ?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?a . "
						+ "?a <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Actor> . }");
		QUERIES_2_COUNT.put(SPARQL_CONSTRUCT.get(2), 1);

		SPARQL_CONSTRUCT
				.add("CONSTRUCT { ?d <http://www.w3.org/2000/01/rdf-schema#seeAlso> ?a } { ?d <http://www.w3.org/2000/01/rdf-schema#label> ?a . }");
		QUERIES_2_COUNT.put(SPARQL_CONSTRUCT.get(3), 42);

		SPARQL_CONSTRUCT.add(
				"CONSTRUCT { ?d <http://www.w3.org/2000/01/rdf-schema#seeAlso> ?a } "
						+ "{ ?d <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> ?a . FILTER isIRI(?a) }");
		QUERIES_2_COUNT.put(SPARQL_CONSTRUCT.get(4), 42);

		SPARQL_QUERIES.addAll(SPARQL_CONSTRUCT);
	}

	protected static Store _tripleStore, _quadStore;
	protected static CumulusRDFSail _sail;
	protected static Repository _repository;

	/**
	 * Shutdown repository and store.
	 * 
	 * @throws Exception mmm...hopefully never otherwise tests will fail.
	 */
	@AfterClass
	public static void shutdown() throws Exception {

		if (_repository != null) {
			_repository.shutDown();
		}

		if (_sail != null) {
			_sail.shutDown();
		}

		if (_tripleStore != null) {
			if (_tripleStore.isOpen()) {
				_tripleStore.clear();
			}

			_tripleStore.close();
		}

		if (_quadStore != null) {
			if (_quadStore.isOpen()) {
				_quadStore.clear();
			}

			_quadStore.close();
		}
	}

	/**
	 * Asserts that a given iterator is not null and is empty.
	 * 
	 * @param query the query that produced the iterator.
	 * @param result the iterator that will be checked.
	 */
	public void assertEmptyIterator(final String query, final Iterator<Statement> result) {
		assertNotNull(query, result);
		assertFalse(query, result.hasNext());
	}
}