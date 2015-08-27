package edu.kit.aifb.cumulus.framework.util;

import static edu.kit.aifb.cumulus.framework.Environment.XML_SCHEMA_DATE;
import static edu.kit.aifb.cumulus.framework.Environment.XML_SCHEMA_DATE_TIME;
import static edu.kit.aifb.cumulus.framework.Environment.XML_SCHEMA_TIME;

import org.joda.time.format.ISODateTimeFormat;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.google.common.base.Predicate;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Booch utility for shared functions.
 * 
 * @author Andreas Wagner
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class Utility {
	private static final HashFunction MURMUR_HASH_3 = Hashing.murmur3_128();

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
		return value == null || value.length == 0;
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
	 * Hashes the given byte[] with MurmurHash3.
	 * 
	 * @param input the input
	 * @return the hash of the input
	 */
	public static HashCode murmurHash3(final byte[] input) {
		return MURMUR_HASH_3.hashBytes(input);
	}
}
