package edu.kit.aifb.cumulus.datasource.serializer;

/**
 * Utility class that infers the concrete Serializer needed to turn a value into its binary representation.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public abstract class SerializerTypeInferer {

	/**
	 * Returns the serializer associated with the given class.
	 * Note: differently from its sibling implementation coming from Hector, 
	 * this class could return null results, if no serializer is found for a given class.
	 * 
	 * @param clazz the class.
	 * @param <T> the type managed by the returned serializer.
	 * @return the serializer associated with the given class, or null if no suitable serializer is found.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> Serializer<T> getSerializer(final Class<?> clazz) {
		Serializer serializer = null;
		if (clazz.equals(byte[].class)) {
			serializer = Serializer.BYTE_ARRAY_SERIALIZER;
		} else if (clazz.equals(Long.class) || clazz.equals(long.class)) {
			serializer = Serializer.LONG_SERIALIZER;
		} else if (clazz.equals(String.class)) {
			serializer = Serializer.STRING_SERIALIZER;
		}
		return serializer;
	}
}
