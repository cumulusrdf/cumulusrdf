package edu.kit.aifb.cumulus.datasource.serializer;

import java.nio.ByteBuffer;

import com.datastax.driver.core.utils.Bytes;

/**
 * Converts {@code <T>} to {@code ByteBuffer} and back.
 * 
 * @param <T> The type to convert to and from.
 * @author Sebastian Schmidt
 * @since 1.1
 */
public abstract class Serializer<T> {
	public static final Serializer<String> STRING_SERIALIZER = new StringSerializer();
	public static final Serializer<byte[]> BYTE_ARRAY_SERIALIZER = new ByteArraySerializer();
	public static final Serializer<Long> LONG_SERIALIZER = new LongSerializer();

	/**
	 * Serializes the given object.
	 * 
	 * @param object The object to serialize.
	 * @return The serialized form of the object, as ByteBuffer.
	 */
	public ByteBuffer serialize(final T object) {
		return ByteBuffer.wrap(serializeInternal(object));
	}

	/**
	 * Serializes the given object.
	 * 
	 * @param object The object to serialize.
	 * @return The serialized form of the object, as byte[].
	 */
	public byte[] serializeDirect(final T object) {
		return serializeInternal(object);
	}

	/**
	 * Serializes the given object into a byte[].
	 * 
	 * @param object The object.
	 * @return A byte[] containing the serialized object.
	 */
	protected abstract byte[] serializeInternal(T object);

	/**
	 * Deserializes the object from the given ByteBuffer.
	 * 
	 * @param serialized The serialized object.
	 * @return The deserialized object.
	 */
	public T deserialize(final ByteBuffer serialized) {
		return deserializeInternal(Bytes.getArray(serialized));
	}

	/**
	 * Deserializes the given byte[].<br />
	 * It is possible that the given array backs a ByteBuffer, so changing it's
	 * content may result in undefined behavior.
	 * 
	 * @param array The byte[]. DO NOT MODIFY THE CONTENT!
	 * @return The deserialized object.
	 * @see ByteBuffer#array()
	 */
	protected abstract T deserializeInternal(byte[] array);

	/**
	 * Returns true if the two values are equal.
	 * 
	 * @param a The first value.
	 * @param b The second value.
	 * @return True if the two values are equal.
	 */
	public boolean isEqual(final T a, final T b) {
		if (a == b) {
			return true;
		}

		if (a == null || b == null) {
			return false;
		}

		return isEqualInternal(a, b);
	}

	/**
	 * Returns true if the two values are equal.
	 * The given values are never null.
	 * 
	 * @param a The first value.
	 * @param b The second value.
	 * @return True if the two values are equal.
	 */
	protected abstract boolean isEqualInternal(T a, T b);
}