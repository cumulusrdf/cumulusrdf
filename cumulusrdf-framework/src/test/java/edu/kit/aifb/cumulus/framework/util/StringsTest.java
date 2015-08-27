package edu.kit.aifb.cumulus.framework.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test case for {@link Strings} utility.
 * 
 * @author Andrea Gazzarini
 * @since 1.1
 */
public class StringsTest {

	static final String[] EMPTY_STRINGS = { "", "   ", "\t\t", "\n\r" };
	static final String[] NOT_EMPTY_STRINGS = { "a", "abc\n", "a\t\ta", "1223\n\r1123    " };

	/**
	 * The method must return true if a given string has a value.
	 * A string, for our purposes, has a value if 
	 * 
	 * <ul>
	 * 	<li>is not null</li>
	 * 	<li>is not an empty string (0 length or only whitespace chars)</li>
	 * </ul>
	 */
	@Test
	public void isNotNullOrEmptyString() {
		for (final String empty : EMPTY_STRINGS) {
			assertFalse(Strings.isNotNullOrEmptyString(empty));
		}

		for (final String empty : NOT_EMPTY_STRINGS) {
			assertTrue(Strings.isNotNullOrEmptyString(empty));
		}
	}
	
	/**
	 * The method must return true if a given string doesn't have a value.
	 * A string, for our purposes, has a value if 
	 * 
	 * <ul>
	 * 	<li>is not null</li>
	 * 	<li>is not an empty string (0 length or only whitespace chars)</li>
	 * </ul>
	 */
	@Test
	public void isNullOrEmptyString() {
		for (final String empty : EMPTY_STRINGS) {
			assertTrue(Strings.isNullOrEmptyString(empty));
		}

		for (final String empty : NOT_EMPTY_STRINGS) {
			assertFalse(Strings.isNullOrEmptyString(empty));
		}
	}
	
}
