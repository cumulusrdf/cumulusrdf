package edu.kit.aifb.cumulus.util;

import static edu.kit.aifb.cumulus.framework.Environment.CHARSET_UTF8;
import static edu.kit.aifb.cumulus.framework.Environment.XML_SCHEMA_DATE;
import static edu.kit.aifb.cumulus.framework.Environment.XML_SCHEMA_DATE_TIME;
import static edu.kit.aifb.cumulus.framework.Environment.XML_SCHEMA_TIME;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.joda.time.format.ISODateTimeFormat;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.StatementCollector;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;

import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * Booch utility for shared functions.
 * 
 * @author Andreas Wagner
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class Util {

	private static final Log LOGGER = new Log(LoggerFactory.getLogger(Util.class));
	static final ValueFactory VALUE_FACTORY = ValueFactoryImpl.getInstance();

	public static final Predicate<Value[]> ALL_CONSTANTS = new Predicate<Value[]>() {

		@Override
		public boolean apply(final Value[] value) {

			for (final Value n : value) {
				if (isVariable(n)) {
					return false;
				}
			}

			return true;
		}
	};

	public static final Predicate<byte[][]> ALL_CONSTANTS_WITH_IDS = new Predicate<byte[][]>() {

		@Override
		public boolean apply(final byte[][] value) {

			for (final byte[] n : value) {
				if (isVariable(n)) {
					return false;
				}
			}

			return true;
		}
	};

	public static final Predicate<Value[]> ALL_VARS = new Predicate<Value[]>() {

		@Override
		public boolean apply(final Value[] value) {

			for (final Value n : value) {
				if (n != null) {
					return false;
				}
			}

			return true;
		}
	};

	public static final Predicate<Value[]> CONTAINS_VAR = new Predicate<Value[]>() {

		@Override
		public boolean apply(final Value[] value) {
			return !ALL_CONSTANTS.apply(value);
		}
	};

	static final String LOCALHOST_NAME = "localhost", LOCALHOST_IP = "127.0.0.1";

	/**
	 * Returns the stacktrace of a given exception.
	 * 
	 * @param throwable the exception.
	 * @return the stacktrace as a string.
	 */
	public static String getStackTrace(final Throwable throwable) {
		final Writer result = new StringWriter();
		throwable.printStackTrace(new PrintWriter(result));
		return result.toString();
	}

	/**
	 * Returns true if the given host represents localhost.
	 * 
	 * @param host  the value to be checked.
	 * @return true if the given host represents localhost.
	 */
	public static boolean isLocalHost(final String host) {
		return (host != null) && (host.equals(LOCALHOST_IP) || host.equals(LOCALHOST_NAME));
	}

	/**
	 * Tests if the parameter 'uri' is a valid URI.
	 * 
	 * @param uri
	 * @return true if parameter 'uri' is a valid URI
	 * @see <a href="http://www.ietf.org/rfc/rfc2396.txt">http://www.ietf.org/rfc/rfc2396.txt</a> 
	 */
	public static boolean isValidURI(String uri) {

		try {

			new java.net.URI(uri);
			return true;

		} catch (URISyntaxException e) {
			return false;
		}
	}

	/**
	 * Returns true if the given byte array represents a variable. The incoming
	 * value is considered a variable if
	 * 
	 * <ul>
	 * <li>Array is null</li>
	 * <li>Array is not null but empty (i.e. size is 0)</li>
	 * <ul>
	 * 
	 * @param value the value to be checked.
	 * @return true if the given value represents a variable.
	 */
	public static boolean isVariable(final byte[] value) {
		return (value == null) || (value.length == 0);
	}

	/**
	 * Returns true if the given value is a variable. A value is considered a
	 * variable if it is null.
	 * 
	 * @param value the value that will be checked.
	 * @return true if the given value is a variable.
	 */
	public static boolean isVariable(final Value value) {
		return value == null;
	}

	/**
	 * Parses the incoming RDF stream according with a given format.
	 * 
	 * @param stream
	 *            the RDF (input) stream.
	 * @param format
	 *            the {@link RDFFormat} that will be used for parsing.
	 * @return an {@link Iterator} over collected statements.
	 */
	public static Iterator<Statement> parseAsIterator(final InputStream stream, final RDFFormat format) {
		return parseAsList(stream, format).iterator();
	}

	/**
	 * Parses the incoming RDF stream according with a given format.
	 * 
	 * @param stream
	 *            the RDF (input) stream.
	 * @param format
	 *            the {@link RDFFormat} that will be used for parsing.
	 * @return a {@link List} with collected statements.
	 */
	public static List<Statement> parseAsList(final InputStream stream, final RDFFormat format) {
		final List<Statement> statements = new ArrayList<Statement>();
		try {
			final RDFParser parser = Rio.createParser(format, VALUE_FACTORY);
			parser.setRDFHandler(new StatementCollector(statements));
			parser.parse(stream, "");
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00029_RDF_PARSE_FAILURE, exception);
		}
		return statements;
	}

	/**
	 * Parses the incoming stream assuming a NT content type.
	 * 
	 * @param stream
	 *            the incoming NT stream.
	 * @return a {@link List} containing collected statements.
	 */
	public static List<Statement> parseNX(final InputStream stream) {
		return parseAsList(stream, RDFFormat.NQUADS);
	}

	/**
	 * Parses the incoming string assuming a NT content type.
	 * 
	 * @param toParse the incoming NT string.
	 * @return a {@link List} containing collected statements.
	 */
	public static List<Statement> parseNX(final String toParse) {
		return parseNX(new ByteArrayInputStream(toParse.getBytes(CHARSET_UTF8)));
	}

	/**
	 * Parses the incoming stream assuming a NT content type.
	 * 
	 * @param stream the incoming NT stream.
	 * @return an {@link Iterator} over collected statements.
	 */
	public static Iterator<Statement> parseNXAsIterator(final InputStream stream) {
		return parseAsIterator(stream, RDFFormat.NQUADS);
	}

	/**
	 * Parses the incoming stream assuming a RDF/XML content type.
	 * 
	 * @param stream the incoming RDF/XML stream.
	 * @return a {@link List} containing collected statements.
	 */
	public static List<Statement> parseXML(final InputStream stream) {
		return parseAsList(stream, RDFFormat.RDFXML);
	}

	/**
	 * Parses the incoming string assuming a RDF/XML content type.
	 * 
	 * @param toParse the input RDF/XML string.
	 * @return a {@link List} containing collected statements.
	 */
	public static List<Statement> parseXML(final String toParse) {
		return parseXML(new ByteArrayInputStream(toParse.getBytes(CHARSET_UTF8)));
	}

	/**
	 * Parses the incoming stream assuming a RDF/XML content type.
	 * 
	 * @param stream the incoming RDF/XML stream.
	 * @return an {@link Iterator} over collected statements.
	 */
	public static Iterator<Statement> parseXMLAsIterator(final InputStream stream) {
		return parseAsIterator(stream, RDFFormat.RDFXML);
	}

	/**
	 * Parses a given literal as a XSD datetime.
	 * 
	 * @param lit the literal.
	 * @return the datetime representation of the given literal (in msecs).
	 */
	public static long parseXMLSchemaDateTimeAsMSecs(final Literal lit) {

		if (lit == null) {
			throw new IllegalArgumentException("literal was null");
		}

		URI dt = lit.getDatatype();

		if (dt == null) {
			throw new IllegalArgumentException("no datatype given");
		}

		if (dt.equals(XML_SCHEMA_DATE)) {

			return ISODateTimeFormat.dateParser().parseMillis(lit.getLabel());

		} else if (dt.equals(XML_SCHEMA_DATE_TIME)) {

			return ISODateTimeFormat.dateTimeParser().parseMillis(lit.getLabel());

		} else if (dt.equals(XML_SCHEMA_TIME)) {

			return ISODateTimeFormat.timeParser().parseMillis(lit.getLabel());

		} else {
			throw new IllegalArgumentException("datatype unknown: " + dt);
		}
	}

	/**
	 * Reorders <i>nodes</i>, an array in SPOC order, to the target order specified by <i>map</i>.
	 * 
	 * @param nodes the array that will be reordered.
	 * @param map the order criteria.
	 * @return a new array reordered according with requested criteria.
	 */
	public static byte[][] reorderQuad(final byte[][] nodes, final int[] map) {
		byte[][] reordered = new byte[4][];
		reordered[0] = nodes[map[0]];
		reordered[1] = nodes[map[1]];
		reordered[2] = nodes[map[2]];
		reordered[3] = nodes[map[3]];

		return reordered;
	}

	/**
	 * Reorders <i>nodes</i> from the order specified by <i>map</i> to SPOC order.
	 * 
	 * @param in the array that will be reordered.
	 * @param map the order criteria.
	 * @return a new array reordered according with requested criteria.
	 */
	public static byte[][] reorderQuadReverse(final byte[][] in, final int[] map) {
		byte[][] reordered = new byte[4][];
		reordered[map[0]] = in[0];
		reordered[map[1]] = in[1];
		reordered[map[2]] = in[2];
		reordered[map[3]] = in[3];

		return reordered;
	}

	/**
	 * Reorders <i>nodes</i>, an array in SPO order, to the target order
	 * specified by <i>map</i>.
	 * 
	 * @param nodes the triple identifiers.
	 * @param map the map that specifies the order criteria.
	 * @return a new array reordered according with the given criteria.
	 */
	public static byte[][] reorderTriple(final byte[][] nodes, final int[] map) {

		byte[][] reordered = new byte[3][];
		reordered[0] = nodes[map[0]];
		reordered[1] = nodes[map[1]];
		reordered[2] = nodes[map[2]];

		return reordered;
	}

	/**
	 * Reorders <i>nodes</i> from the order specified by <i>map</i> to SPO order.
	 * 
	 * @param in the input byte array.
	 * @param map the map containing reorder criteria.
	 * @return a new input array reordered according with input criteria.
	 */
	public static byte[][] reorderTripleReverse(final byte[][] in, final int[] map) {

		final byte[][] reordered = new byte[3][];
		reordered[map[0]] = in[0];
		reordered[map[1]] = in[1];
		reordered[map[2]] = in[2];

		return reordered;
	}

	/**
	 * Returns a singleton iterator with a single item.
	 * 
	 * @param <T> the item kind.
	 * @param item the item.
	 * @return a singleton iterator with a single item.
	 */
	public static <T> Iterator<T> singletonIterator(final T item) {

		return new Iterator<T>() {

			private T _item = item;
			private boolean _hasItem = false;

			@Override
			public boolean hasNext() {
				return !_hasItem;
			}

			@Override
			public T next() {
				if (_hasItem) {
					throw new NoSuchElementException();
				}
				_hasItem = true;
				return _item;
			}

			@Override
			public void remove() {
				if (!_hasItem) {
					_hasItem = true;
				} else {
					throw new NoSuchElementException();
				}
			}
		};
	}

	public static <T> Iterator<T> toResultIterator (final RepositoryResult<T> result) {
		
		return new AbstractIterator<T>() {

			@Override
			protected T computeNext() {
				
				try {
					
					if(result.hasNext()) {
						return result.next();
					}
					
				} catch (RepositoryException e) {
					LOGGER.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG + " Could not compute next result.", e);
				}
				return endOfData();
			}
		};
	}
	
	/**
	 * Converts a statement to an array of its RDF values.
	 * 
	 * @param stmt - statement to be converted
	 * @return array containing the RDF values comprised in the statement.
	 */
	public static Value[] toValueArray(Statement stmt) {

		Value[] out = new Value[stmt.getContext() == null ? 3 : 4];
		out[0] = stmt.getSubject();
		out[1] = stmt.getPredicate();
		out[2] = stmt.getObject();

		if (stmt.getContext() != null) {
			out[3] = stmt.getContext();
		}

		return out;
	}
}