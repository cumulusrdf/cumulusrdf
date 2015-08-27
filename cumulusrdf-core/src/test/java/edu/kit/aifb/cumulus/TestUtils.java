package edu.kit.aifb.cumulus;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResult;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.datasource.StorageLayout;
import edu.kit.aifb.cumulus.store.CumulusStoreException;
import edu.kit.aifb.cumulus.store.QuadStore;
import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.TripleStore;

/**
 * Test data and utilities.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class TestUtils {
	public static final DataAccessLayerFactory T_DATA_ACCESS_LAYER_FACTORY = DataAccessLayerFactory.getDefaultDataAccessLayerFactory(StorageLayout.TRIPLE);
	public static final DataAccessLayerFactory Q_DATA_ACCESS_LAYER_FACTORY = DataAccessLayerFactory.getDefaultDataAccessLayerFactory(StorageLayout.QUAD);
	
	public static final ValueFactory VALUE_FACTORY = ValueFactoryImpl.getInstance();

	public static final Random RANDOMIZER = new Random(System.currentTimeMillis());
	public static final String[] EMPTY_STRINGS = { "", " ", "\n\r", "\t" };

	public static final String EXAMPLE_CONFIG_FILE = "/cumulus.yaml";

	public static final Value[] SELECT_ALL_TRIPLES_PATTERN = { null, null, null };
	public static final Value[] SELECT_ALL_QUADS_PATTERN = { null, null, null, null };

	public static final String STORE_LISTEN_ADDRESS = "localhost:9161";
	public static final String TRIPLE_STORE_KEYSPACE_NAME = "KeyspaceCumulusTriple";
	public static final String QUAD_STORE_KEYSPACE_NAME = "KeyspaceCumulusQuad";

	/**
	 * Converts a given iterator in a list.
	 * @param <T>
	 * 
	 * @param iterator the iterator.
	 * @param <T> the iterator element type.
	 * @return a list containing elements found in the iterator.
	 */
	public static <T> List<T> asList(final Iterator<T> iterator) {
		final List<T> result = new ArrayList<T>();
		while (iterator.hasNext()) {
			result.add(iterator.next());
		}

		return result;
	}

	/**
	 * Builds a bnode.
	 * 
	 * @param id the blank node identifier.
	 * @return a blank node. 
	 */
	public static BNode buildBNode(final Object id) {
		return VALUE_FACTORY.createBNode(String.valueOf(id));
	}
	
	/**
	 * Builds a literal with the given data.
	 * 
	 * @param data the literal value.
	 * @return a literal. 
	 */
	public static Literal buildLiteral(final String data) {
		return VALUE_FACTORY.createLiteral(data);
	}
	
	/**
	 * Builds a datatyped literal.
	 * 
	 * @param data the literal value.
	 * @param datatype the literal type.
	 * @return a datatyped literal. 
	 */
	public static Literal buildLiteral(final String data, final URI datatype) {
		return VALUE_FACTORY.createLiteral(data, datatype);
	}
	
	/**
	 * Builds a new {@link URI} from a given string.
	 * 
	 * @param name the resource name.
	 * @return a new {@link URI} resource.
	 */
	public static URI buildResource(final String name) {
		return name.startsWith("http") ? VALUE_FACTORY.createURI(name) : VALUE_FACTORY.createURI("http://cumulus/" + name);
	}

	/**
	 * Cleans up the given store.
	 * 
	 * @param crdf could be a triple or a quad store.
	 * @throws CumulusStoreException in case the cleanup fails.
	 */
	public static void clean(final Store crdf) throws CumulusStoreException {
		final Value[] pattern = crdf instanceof TripleStore ? SELECT_ALL_TRIPLES_PATTERN : SELECT_ALL_QUADS_PATTERN;
		crdf.removeData(pattern);
		assertEquals("Store seems not empty after issuing a delete * command.", 0, numOfRes(crdf.query(pattern)));
	}

	/**
	 * Returns a new instance of a quad store with default values.
	 * 
	 * @return a new instance of a quad store with default values.
	 */
	public static final Store newQuadStore() {
		return new QuadStore(randomString());
	}

	/**
	 * Creates a new statement with the given data.
	 * 
	 * @param localSubjectName the local subject name.
	 * @param localPredicateName the local predicate name.
	 * @param localObjectName the local object name.
	 * @param localContextName the local context name.
	 * @return a new statement.
	 */
	public static Statement newStatement(
			final String localSubjectName, 
			final String localPredicateName, 
			final String localObjectName, 
			final String localContextName) {
		
		return VALUE_FACTORY.createStatement(
				buildResource(localSubjectName), 
				buildResource(localPredicateName), 
				buildResource(localObjectName),
				buildResource(localContextName));
	}
	
	

	/**
	 * Returns a new instance of a triple store with default values.
	 * 
	 * @return a new instance of a triple store with default values.
	 */
	public static final Store newTripleStore() {
		return new TripleStore(randomString());
	}

	/**
	 * Returns how many triples are in the given iterator.
	 * @param <T>
	 * 
	 * @param nodes the iterator.
	 * @param <T> the iterator element type.
	 * @return how many triples are in the given iterator.
	 */
	public static <T> int numOfRes(final Iterator<T> nodes) {
		int numOfTriples = 0;

		while (nodes.hasNext()) {
			nodes.next();
			numOfTriples++;
		}

		return numOfTriples;
	}

	/**
	 * Returns how many triples are in the given query result..
	 * 
	 * @param result the query result.
	 * @param <T> the query result element type.
	 * @return how many triples are in the given iterator.
	 * @throws QueryEvaluationException in case of failure while iterating the query result.
	 */
	public static <T> int numOfRes(final QueryResult<T> result) throws QueryEvaluationException {

		int numOfTriples = 0;

		while (result.hasNext()) {
			result.next();
			numOfTriples++;
		}

		return numOfTriples;
	}

	/**
	 * Generates a random int.
	 * 
	 * @return a random int.
	 */
	public static final int randomInt() {
		return RANDOMIZER.nextInt();
	}

	/**
	 * Generates a random long.
	 * 
	 * @return a random long.
	 */
	public static final long randomLong() {
		return RANDOMIZER.nextLong();
	}

	/**
	 * Creates a list of random statements.
	 * @param random The pseudo random generator used for string generation.
	 * @param amount The amount of statements to generate.
	 * @return A list with random statements.
	 */
	public static List<Statement> randomStatements(final Random random, final int amount) {
		List<Statement> list = new ArrayList<Statement>(amount);

		for (int i = 0; i < amount; i++) {
			list.add(new ContextStatementImpl(
					new URIImpl("http://a.b/" + randomString(random, 32)),
					new URIImpl("http://a.b/" + randomString(random, 32)),
					new URIImpl("http://a.b/" + randomString(random, 32)),
					new URIImpl("http://a.b/" + randomString(random, 32))));
		}

		return list;
	}

	/**
	 * Generates a random string.
	 * 
	 * @return a random string.
	 */
	public static final String randomString() {
		final long random = RANDOMIZER.nextLong() + 1;
		return String.valueOf(random > 0 ? random : (random * -1));
	}

	/**
	 * Returns a random string constructed with the given {@code Random} object and the specified length.
	 * The string will contain characters matching [a-zA-Z0-9].
	 * 
	 * @param random The pseudo random generator used for string generation.
	 * @param length The length of the string to return.
	 * @return A random string with the given length.
	 */
	public static String randomString(final Random random, final int length) {
		StringBuilder str = new StringBuilder(length);
		String allowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

		for (int i = 0; i < length; i++) {
			str.append(allowedChars.charAt(random.nextInt(allowedChars.length())));
		}

		return str.toString();
	}
	
	/**
	 * Converts a statement iterator to an input stream with serialized RDF data.
	 * 
	 * @param statements The statement iterator.
	 * @param format The RDF format to use for serialization.
	 * @return The serialized RDF data.
	 * @throws RDFHandlerException in case of operation failure. 
	 */
	public static InputStream statementIteratorToRdfStream(final Iterator<Statement> statements, final RDFFormat format) throws RDFHandlerException {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		RDFWriter rdfWriter = Rio.createWriter(format, stream);

		rdfWriter.startRDF();

		while (statements.hasNext()) {
			rdfWriter.handleStatement(statements.next());
		}

		rdfWriter.endRDF();
		return new ByteArrayInputStream(stream.toByteArray());
	}

}