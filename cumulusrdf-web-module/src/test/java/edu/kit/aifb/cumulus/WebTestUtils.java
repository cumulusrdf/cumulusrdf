package edu.kit.aifb.cumulus;

import static org.junit.Assert.assertEquals;
import info.aduna.iteration.Iterations;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResult;
import org.openrdf.query.TupleQueryResult;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.datasource.StorageLayout;
import edu.kit.aifb.cumulus.store.CumulusStoreException;
import edu.kit.aifb.cumulus.store.QuadStore;
import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.TripleStore;
import edu.kit.aifb.cumulus.util.Util;

/**
 * Test data and utilities.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class WebTestUtils {
	public static class LineSeparator {
		// Windows: \r\n Unix: \n Mac: \r
		public static final String WIN = "\r\n", MAC = "\r", UNIX = "\n";
		public static final String[] ALL = new String[] { WIN, MAC, UNIX };
	}

	public static final DataAccessLayerFactory T_DATA_ACCESS_LAYER_FACTORY = DataAccessLayerFactory.getDefaultDataAccessLayerFactory(StorageLayout.TRIPLE);

	public static final DataAccessLayerFactory Q_DATA_ACCESS_LAYER_FACTORY = DataAccessLayerFactory.getDefaultDataAccessLayerFactory(StorageLayout.QUAD);

	static final ValueFactory VALUE_FACTORY = ValueFactoryImpl.getInstance();
	public static final Random RANDOMIZER = new Random(System.currentTimeMillis());

	public static final String[] EMPTY_STRINGS = { "", " ", "\n\r", "\t" };

	private static final String TMP_FILE = "./target/testing/tmp.txt";
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
	 * Copied from org.openrdf.query.QueryResultUtil
	 */
	private static boolean bindingSetsMatch(final BindingSet bs1, final BindingSet bs2) {

		if (bs1.size() != bs2.size()) {
			return false;
		}

		for (Binding binding1 : bs1) {
			Value value1 = binding1.getValue();
			Value value2 = bs2.getValue(binding1.getName());

			if ((value1 instanceof BNode) && (value2 instanceof BNode)) {
				// BNode mappedBNode = bNodeMapping.get(value1);
				//
				// if (mappedBNode != null) {
				// // bNode 'value1' was already mapped to some other bNode
				// if (!value2.equals(mappedBNode)) {
				// // 'value1' and 'value2' do not match
				// return false;
				// }
				// } else {
				// // 'value1' was not yet mapped, we need to check if 'value2'
				// // is a
				// // possible mapping candidate
				// if (bNodeMapping.containsValue(value2)) {
				// // 'value2' is already mapped to some other value.
				// return false;
				// }
				// }

				return value1.equals(value2);
			} else {
				// values are not (both) bNodes
				if ((value1 instanceof Literal) && (value2 instanceof Literal)) {
					// do literal value-based comparison for supported datatypes
					Literal leftLit = (Literal) value1;
					Literal rightLit = (Literal) value2;

					URI dt1 = leftLit.getDatatype();
					URI dt2 = rightLit.getDatatype();

					if ((dt1 != null) && (dt2 != null) && dt1.equals(dt2) && XMLDatatypeUtil.isValidValue(leftLit.getLabel(), dt1)
							&& XMLDatatypeUtil.isValidValue(rightLit.getLabel(), dt2)) {
						Integer compareResult = null;
						if (dt1.equals(XMLSchema.DOUBLE)) {
							compareResult = Double.compare(leftLit.doubleValue(), rightLit.doubleValue());
						} else if (dt1.equals(XMLSchema.FLOAT)) {
							compareResult = Float.compare(leftLit.floatValue(), rightLit.floatValue());
						} else if (dt1.equals(XMLSchema.DECIMAL)) {
							compareResult = leftLit.decimalValue().compareTo(rightLit.decimalValue());
						} else if (XMLDatatypeUtil.isIntegerDatatype(dt1)) {
							compareResult = leftLit.integerValue().compareTo(rightLit.integerValue());
						} else if (dt1.equals(XMLSchema.BOOLEAN)) {
							Boolean leftBool = Boolean.valueOf(leftLit.booleanValue());
							Boolean rightBool = Boolean.valueOf(rightLit.booleanValue());
							compareResult = leftBool.compareTo(rightBool);
						} else if (XMLDatatypeUtil.isCalendarDatatype(dt1)) {
							XMLGregorianCalendar left = leftLit.calendarValue();
							XMLGregorianCalendar right = rightLit.calendarValue();

							compareResult = left.compare(right);
						}

						if (compareResult != null) {
							if (compareResult.intValue() != 0) {
								return false;
							}
						} else if (!value1.equals(value2)) {
							return false;
						}
					} else if (!value1.equals(value2)) {
						return false;
					}
				} else if (!value1.equals(value2)) {
					return false;
				}
			}
		}

		return true;
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
	 * @param value the uri as a string.
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
		crdf.clear();
		assertEquals("Store seems not empty after issuing a delete * command.", 0,
				numOfRes(crdf.query(crdf instanceof TripleStore ? SELECT_ALL_TRIPLES_PATTERN : SELECT_ALL_QUADS_PATTERN)));
	}

	public static String contentAsString(File file, String lineSepartor) throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = null;
		StringBuilder sb = new StringBuilder();

		while ((line = br.readLine()) != null) {
			sb.append(line.trim());
			sb.append(System.getProperty("line.separator"));
		}

		br.close();
		return sb.toString();
	}

	public static String idToString(byte[] id) {
		StringBuilder sb = new StringBuilder();

		for (byte b : id) {
			sb.append(String.format("%02X ", b));
		}

		return sb.toString();
	}

	public static Iterator<Statement> loadNTriplesFromFile(File file) throws FileNotFoundException {
		return Util.parseNXAsIterator(new FileInputStream(file));
	}

	public static Iterator<Statement> loadNTriplesFromFile(String path) throws FileNotFoundException {
		return loadNTriplesFromFile(new File(path));
	}

	/**
	 * @see org.openrdf.query.QueryResultUtil
	 */
	public static boolean matchTupleQueryResults(TupleQueryResult res1, TupleQueryResult res2) throws QueryEvaluationException {

		List<BindingSet> queryResult1 = Iterations.asList(res1);
		List<BindingSet> queryResult2 = Iterations.asList(res2);

		if (queryResult1.size() != queryResult2.size()) {
			return false;
		}

		for (BindingSet bs1 : queryResult1) {

			boolean hit = false;

			for (BindingSet bs2 : queryResult2) {

				if (bindingSetsMatch(bs1, bs2)) {
					hit = true;
					break;
				}
			}

			if (!hit) {
				return false;
			}
		}

		return true;
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

	public static <T> int numOfRes(QueryResult<T> result) throws QueryEvaluationException {

		int numOfTriples = 0;

		while (result.hasNext()) {
			result.next();
			numOfTriples++;
		}

		return numOfTriples;
	}

	public static int printIds(Iterator<byte[][]> ids, PrintStream stream) {
		if (!ids.hasNext()) {
			stream.println("nodes iterator empty");
			return 0;
		}

		int numOfTriples = 0;

		while (ids.hasNext()) {
			byte[][] next_triple = ids.next();

			if (next_triple.length == 4) {
				stream.println(idToString(next_triple[0]) + " " + idToString(next_triple[1]) + " " + idToString(next_triple[2]) + " " + idToString(next_triple[3]) + " . ");
			} else {
				stream.println(idToString(next_triple[0]) + " " + idToString(next_triple[1]) + " " + idToString(next_triple[2]) + " . ");
			}

			numOfTriples++;
		}

		return numOfTriples;
	}

	public static int printNQ(Iterator<Value[]> nodes, PrintStream stream) {

		if (!nodes.hasNext()) {
			stream.println("nodes iterator empty");
			return 0;
		}

		int numOfTriples = 0;

		while (nodes.hasNext()) {
			Value[] next_triple = nodes.next();
			stream.println(next_triple[0].toString() + " " + next_triple[1].toString() + " " + next_triple[2].toString() + " " + next_triple[3].toString() + " . ");
			numOfTriples++;
		}

		return numOfTriples;
	}

	public static int printNT(Iterator<Value[]> nodes, PrintStream stream) {

		if (!nodes.hasNext()) {
			stream.println("nodes iterator empty");
			return 0;
		}

		int numOfTriples = 0;

		while (nodes.hasNext()) {
			Value[] next_triple = nodes.next();
			stream.println(next_triple[0].toString() + " " + next_triple[1].toString() + " " + next_triple[2].toString() + " . ");
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
	 * Generates a random string.
	 * 
	 * @return a random string.
	 */
	public static final String randomString() {
		return String.valueOf(RANDOMIZER.nextLong());
	}

	/**
	 * Creates a tmp file under build directory.
	 * 
	 * @return a reference to a temporary file.
	 * @throws IOException
	 *             in case the file cannot be created.
	 */
	public static File tmpFile() throws IOException {

		final File tmp = new File(TMP_FILE);
		final File parentDirectory = tmp.getParentFile();

		if (!parentDirectory.exists()) {
			parentDirectory.mkdirs();
		}

		if (tmp.exists()) {
			tmp.delete();
		}

		if (tmp.createNewFile()) {
			return tmp;
		} else {
			throw new IOException("File " + tmp.getAbsolutePath() + " cannot be created.");
		}
	}
}