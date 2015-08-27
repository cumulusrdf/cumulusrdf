package edu.kit.aifb.cumulus.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;

import edu.kit.aifb.cumulus.TestUtils;

/**
 * Test case for {@link Util}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0.0
 */
public class UtilTest {

	/**
	 * A value is considered a variable if it is null.
	 */
	@Test
	public void valuesIsVariable() {
		assertTrue(Util.isVariable((Value) null));
		assertFalse(Util.isVariable(new LiteralImpl("This is a literal")));
	}

	/**
	 * A value (as byte array) is considered a variable. This happens in the
	 * following cases:
	 * 
	 * <ul>
	 * <li>Array is null</li>
	 * <li>Array is not null but empty (i.e. size is 0)</li>
	 * <li>Array is not null and the first element corresponds to NodeDictionaryBase#NULL_ID[0]</li>
	 * <li>Array is not null and the first element corresponds to NodeDictionaryBase#VARIABLE_ID}[0]</li>
	 * <ul>
	 */
	@Test
	public void valueAsByteArrayIsVariable() {
		final byte[][] nullValues = { null, {} };

		for (final byte[] nullValue : nullValues) {
			assertTrue(Util.isVariable(nullValue));
		}
	}

	/**
	 * A given string identifies a localhost address.
	 */
	@Test
	public void isLocalhost() {
		assertTrue(Util.isLocalHost(Util.LOCALHOST_IP));
		assertTrue(Util.isLocalHost(Util.LOCALHOST_NAME));

		assertFalse(Util.isLocalHost(Util.LOCALHOST_IP.replaceAll("0", String.valueOf(TestUtils.RANDOMIZER.nextInt(255)))));
		assertFalse(Util.isLocalHost(String.valueOf(System.currentTimeMillis())));
	}
}