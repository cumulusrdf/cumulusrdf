package edu.kit.aifb.cumulus.framework.util;

/**
 * Booch utility for string manipulation.
 * 
 * @author Andrea Gazzarini
 * @since 1.1
 */
public abstract class Strings {
	
	public static final String EMPTY_STRING = "";
	
	/**
	 * Checks if the given string is not null or (not) empty string.
	 * 
	 * @param value the string to check.
	 * @return true if the string is not null and is not empty string
	 */
	public static boolean isNotNullOrEmptyString(final String value) {
		return value != null && value.trim().length() != 0;
	}
	
	/**
	 * Checks if the given string is null or empty string.
	 * 
	 * @param value the string to check.
	 * @return true if the string is not null and is not empty string
	 */
	public static boolean isNullOrEmptyString(final String value) {
		return value == null || value.trim().length() == 0;
	}	

	/**
	 * Returns the given value as an int.
	 * 
	 * @param value the string that will be converted.
	 * @param defaultValue in case the conversion fails this value will be returned.
	 * @return the given value as an int.
	 */
	public static int asInt(final String value, final int defaultValue) {
		try {
			return Integer.parseInt(value);
		} catch (final Exception exception) {
			return defaultValue;
		}
	}
}