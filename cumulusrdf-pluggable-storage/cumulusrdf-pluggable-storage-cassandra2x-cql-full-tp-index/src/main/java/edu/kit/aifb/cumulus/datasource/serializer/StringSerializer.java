package edu.kit.aifb.cumulus.datasource.serializer;

import java.io.UnsupportedEncodingException;

import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * A string serializer.
 * 
 * @author Sebastian Schmidt
 * @since 1.1.0
 */
public class StringSerializer extends Serializer<String> {
	@Override
	public byte[] serializeInternal(final String object) {
		try {
			return object.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			new Log(LoggerFactory.getLogger(getClass())).error(MessageCatalog._00104_UTF8_NOT_SUPPORTED, e);
			return null;
		}
	}

	@Override
	public String deserializeInternal(final byte[] serialized) {
		try {
			return new String(serialized, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			new Log(LoggerFactory.getLogger(getClass())).error(MessageCatalog._00104_UTF8_NOT_SUPPORTED, e);
			return null;
		}
	}

	@Override
	protected boolean isEqualInternal(final String a, final String b) {
		return a.equals(b);
	}
}