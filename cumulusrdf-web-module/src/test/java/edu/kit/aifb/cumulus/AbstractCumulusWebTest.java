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
 * Abstract class for all CumulusRDF test cases.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class AbstractCumulusWebTest implements WebTestData {

	public static final String DATA_NT = "src/test/resources/triples_gridpedia.nt";
	public static List<String> _sparql_queries, _sparql_ask, _sparql_select, _sparql_construct;
	public static Map<String, Integer> _queries2count;

	static {

		_sparql_queries = new LinkedList<String>();
		_queries2count = new HashMap<String, Integer>();

		_sparql_ask = new LinkedList<String>();
		_sparql_ask.add("ASK { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . }");
		_sparql_ask.add("ASK { ?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> . }");
		_sparql_ask
				.add("ASK { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . ?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> " +
						"<http://gridpedia.org/id/Actor> . }");
		_sparql_ask.add("ASK { <http://gridpedia.org/id/Media> <http://semantic-mediawiki.org/swivt/1.0#page> ?d . }");
		_sparql_queries.addAll(_sparql_ask);

		_sparql_select = new LinkedList<String>();

		_sparql_select
				.add("SELECT * WHERE { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . ?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> . }");
		_queries2count.put(_sparql_select.get(0), 1);

		_sparql_select
				.add("SELECT * WHERE { ?a <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Actor> . ?a <http://www.w3.org/2000/01/rdf-schema#label> ?l . ?a ?p ?o . }");
		_queries2count.put(_sparql_select.get(1), 10);

		_sparql_select
				.add("SELECT * WHERE { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . ?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?a . ?a <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Actor> .}");
		_queries2count.put(_sparql_select.get(2), 1);

		_sparql_select
				.add("SELECT * WHERE { ?d <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Media> . ?d <http://semantic-mediawiki.org/swivt/1.0#wikiPageModificationDate> ?date . FILTER ( ?date >= \"2012-01-30T00:00:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ) } LIMIT 1 ");
		_queries2count.put(_sparql_select.get(3), 1);

		_sparql_select.add("SELECT * WHERE { ?d <http://semantic-mediawiki.org/swivt/1.0#page> ?p . } ORDER BY DESC(?p) ");
		_queries2count.put(_sparql_select.get(4), 42);

		_sparql_select.add("SELECT DISTINCT ?d WHERE { ?d <http://semantic-mediawiki.org/swivt/1.0#page> ?p . } ");
		_queries2count.put(_sparql_select.get(5), 42);

		_sparql_select.add("SELECT * WHERE { ?d <http://semantic-mediawiki.org/swivt/1.0#page> ?p . } ORDER BY DESC(?p) LIMIT 1 ");
		_queries2count.put(_sparql_select.get(6), 1);

		_sparql_select
				.add("SELECT * WHERE { ?d <http://semantic-mediawiki.org/swivt/1.0#page> ?p . OPTIONAL { ?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> } }");
		_queries2count.put(_sparql_select.get(7), 42);

		_sparql_select
				.add("SELECT * WHERE { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device2\" . ?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor> . }");
		_queries2count.put(_sparql_select.get(8), 0);

		_sparql_select.add("SELECT ?d WHERE { ?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://gridpedia.org/id/Actor2> . }");
		_queries2count.put(_sparql_select.get(9), 0);

		_sparql_queries.addAll(_sparql_select);

		_sparql_construct = new LinkedList<String>();
		_sparql_construct
				.add("CONSTRUCT { ?d <http://xmlns.com/foaf/0.1/name> \"Device\" } { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . }");
		_queries2count.put(_sparql_construct.get(0), 1);

		_sparql_construct
				.add("CONSTRUCT { ?d <http://xmlns.com/foaf/0.1/name> \"Device\" } { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . ?d <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Device> . }");
		_queries2count.put(_sparql_construct.get(1), 1);

		_sparql_construct
				.add("CONSTRUCT { ?d <http://www.w3.org/2000/01/rdf-schema#seeAlso> ?a } { ?d <http://www.w3.org/2000/01/rdf-schema#label> \"Device\" . ?d <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?a . ?a <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Actor> . }");
		_queries2count.put(_sparql_construct.get(2), 1);

		_sparql_construct
				.add("CONSTRUCT { ?d <http://www.w3.org/2000/01/rdf-schema#seeAlso> ?a } { ?d <http://www.w3.org/2000/01/rdf-schema#label> ?a . }");
		_queries2count.put(_sparql_construct.get(3), 42);

		_sparql_construct
				.add("CONSTRUCT { ?d <http://www.w3.org/2000/01/rdf-schema#seeAlso> ?a } { ?d <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> ?a . FILTER isIRI(?a) }");
		_queries2count.put(_sparql_construct.get(4), 42);

		_sparql_queries.addAll(_sparql_construct);
	}

	public static Store TRIPLE_STORE, QUAD_STORE;

	protected static CumulusRDFSail SAIL;
	protected static Repository REPOSITORY;

	/**
	 * Shutdown repository and store.
	 * 
	 * @throws Exception mmm...hopefully never otherwise tests will fail.
	 */
	@AfterClass
	public static void shutdown() throws Exception {
		
		if (REPOSITORY != null) {
			REPOSITORY.shutDown();
		}

		if (SAIL != null) {
			SAIL.shutDown();
		}
		
		if (TRIPLE_STORE != null) {
			TRIPLE_STORE.close();
		}
		
		if (QUAD_STORE != null) {
			QUAD_STORE.close();
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