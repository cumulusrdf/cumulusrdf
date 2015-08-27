package edu.kit.aifb.cumulus.datasource.serializer;

import edu.kit.aifb.cumulus.framework.util.Bytes;

/**
 * A long serializer.
 * 
 * @author Sebastian Schmidt
 * @since 1.1.0
 */
public class LongSerializer extends Serializer<Long> {
	@Override
	protected byte[] serializeInternal(final Long object) {
		byte[] result = new byte[8];
		Bytes.encode(object, result, 0);
		return result;
	}

	@Override
	protected Long deserializeInternal(final byte[] array) {
		return Bytes.decodeLong(array, 0);
	}

	@Override
	protected boolean isEqualInternal(final Long a, final Long b) {
		return a == b;
	}
}