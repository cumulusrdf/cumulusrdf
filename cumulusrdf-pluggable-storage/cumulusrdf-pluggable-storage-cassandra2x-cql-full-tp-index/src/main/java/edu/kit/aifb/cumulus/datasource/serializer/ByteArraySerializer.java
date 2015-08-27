package edu.kit.aifb.cumulus.datasource.serializer;

import java.util.Arrays;

/**
 * A byte array serializer.
 * 
 * @author Sebastian Schmidt
 * @since 1.1.0
 */
public class ByteArraySerializer extends Serializer<byte[]> {

	@Override
	public byte[] serializeInternal(final byte[] object) {
		return object;
	}

	@Override
	public byte[] deserializeInternal(final byte[] serialized) {
		return serialized;
	}

	@Override
	protected boolean isEqualInternal(final byte[] a, final byte[] b) {
		return Arrays.equals(a, b);
	}
}