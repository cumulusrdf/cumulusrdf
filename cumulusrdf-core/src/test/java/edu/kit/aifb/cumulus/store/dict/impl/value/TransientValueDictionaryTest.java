package edu.kit.aifb.cumulus.store.dict.impl.value;

import static edu.kit.aifb.cumulus.TestUtils.T_DATA_ACCESS_LAYER_FACTORY;
import static edu.kit.aifb.cumulus.TestUtils.VALUE_FACTORY;
import static edu.kit.aifb.cumulus.TestUtils.randomString;
import static edu.kit.aifb.cumulus.framework.Environment.CHARSET_UTF8;
import static edu.kit.aifb.cumulus.framework.util.Bytes.decodeShort;
import static edu.kit.aifb.cumulus.framework.util.Bytes.fillIn;
import static edu.kit.aifb.cumulus.framework.util.Bytes.subarray;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.BNode;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.ntriples.NTriplesUtil;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.domain.dictionary.IDictionary;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;

/**
 * Test case for {@link TransientValueDictionary}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class TransientValueDictionaryTest {

	final Value _firstMember = VALUE_FACTORY.createURI("http://pippo.pluto.paperino#first");
	final Value _secondMember = VALUE_FACTORY.createURI("http://pippo.pluto.paperino#second");
	final Value _thirdMember = VALUE_FACTORY.createURI("http://pippo.pluto.paperino#third");
	final Value _fourthMember = VALUE_FACTORY.createLiteral("Hello there! It's Gazza!");
	final Value _fifthMember = VALUE_FACTORY.createBNode(String.valueOf(System.currentTimeMillis()));

	Value _longLiteral;
	
	byte[] _longLiteralIdGeneratedByEmbeddedDictionary;
	byte[] _longLiteralId;
	
	private TransientValueDictionary _cut;
	
	/**
	 * Setup fixture for this test.
	 * 
	 * @throws Exception never, otherwise the corresponding test will fail.
	 */
	@Before
	public void setUp() throws Exception {

		_longLiteralId = new byte[PersistentValueDictionary.ID_LENGTH + 1];
		_longLiteralId[0] = TransientValueDictionary.THRESHOLD_EXCEEDED;
		_longLiteralId[1] = ValueDictionaryBase.LITERAL_BYTE_FLAG;

		_longLiteralIdGeneratedByEmbeddedDictionary = new byte[PersistentValueDictionary.ID_LENGTH];
		_longLiteralIdGeneratedByEmbeddedDictionary[0] = ValueDictionaryBase.LITERAL_BYTE_FLAG;
		for (int i = 1; i < PersistentValueDictionary.ID_LENGTH; i++) {
			_longLiteralIdGeneratedByEmbeddedDictionary[i] = (byte) i;
		}

		for (int i = 1; i < PersistentValueDictionary.ID_LENGTH; i++) {
			_longLiteralId[i + 1] = (byte) i;
		}

		_cut = new TransientValueDictionary(randomString())
		{
			@Override
			public void initialiseInternal(final DataAccessLayerFactory factory) {
				_longLiteralsDictionary = new ValueDictionaryBase(randomString()) {
					
					@Override
					public void removeValue(final Value value, final boolean p) {
						// Nothing here
					}

					@Override
					public byte[] getIdInternal(final Value value, final boolean p) {
						return _longLiteralIdGeneratedByEmbeddedDictionary;
					}

					@Override
					public void initialiseInternal(final DataAccessLayerFactory factory) {
						// Nothing to be done here...
					}

					@Override
					public byte[][] decompose(final byte[] compositeId) {
						// Never called for this test.
						return null;
					}
					
					@Override
					public byte[] compose(final byte[] id1, final byte[] id2, final byte[] id3) {
						// Never called for this test.
						return null;
					}
					
					@Override
					public byte[] compose(final byte[] id1, final byte[] id2) {
						// Never called for this test.
						return null;
					}
					
					@Override
					public void closeInternal() {
						// Never called for this test.
					}

					@Override
					public Value getValueInternal(final byte[] id, final boolean p) {
						return _longLiteral;
					}

					@Override
					public boolean isBNode(final byte[] id) {
						return false;
					}

					@Override
					public boolean isLiteral(final byte[] id) {
						return false;
					}

					@Override
					public boolean isResource(final byte[] id) {
						return false;
					}
				};
			}
		};

		_cut.initialise(T_DATA_ACCESS_LAYER_FACTORY);
		
		final StringBuilder builder = new StringBuilder();
		for (int i = 0; i < _cut._threshold + 1; i++) {
			builder.append('a');
		}

		_longLiteral = VALUE_FACTORY.createLiteral(builder.toString());
	}
	
	/**
	 * Tests if an identifier is a blank node or not.
	 */
	@Test
	public void isBNode() {
		assertFalse(_cut.isBNode(null));
		assertFalse(_cut.isBNode(new byte[] { TransientValueDictionary.THRESHOLD_EXCEEDED }));
		assertFalse(_cut.isBNode(new byte[] { TransientValueDictionary.THRESHOLD_NOT_EXCEEDED, IDictionary.LITERAL_BYTE_FLAG }));
		assertFalse(_cut.isBNode(new byte[] { TransientValueDictionary.THRESHOLD_NOT_EXCEEDED, IDictionary.RESOURCE_BYTE_FLAG }));

		assertTrue(_cut.isBNode(new byte[] { TransientValueDictionary.THRESHOLD_NOT_EXCEEDED, IDictionary.BNODE_BYTE_FLAG }));
	}

	/**
	 * Tests if an identifier is a literal or not.
	 */
	@Test
	public void isLiteral() {
		assertFalse(_cut.isLiteral(null));
		assertFalse(_cut.isLiteral(new byte[] { TransientValueDictionary.THRESHOLD_NOT_EXCEEDED, IDictionary.BNODE_BYTE_FLAG }));
		assertFalse(_cut.isLiteral(new byte[] { TransientValueDictionary.THRESHOLD_NOT_EXCEEDED, IDictionary.RESOURCE_BYTE_FLAG }));

		assertTrue(_cut.isLiteral(new byte[] { TransientValueDictionary.THRESHOLD_EXCEEDED }));
		assertTrue(_cut.isLiteral(new byte[] { TransientValueDictionary.THRESHOLD_NOT_EXCEEDED, IDictionary.LITERAL_BYTE_FLAG }));
	}

	/**
	 * Tests if an identifier is a resource or not.
	 */
	@Test
	public void isResource() {
		assertFalse(_cut.isResource(null));
		assertFalse(_cut.isResource(new byte[] { TransientValueDictionary.THRESHOLD_EXCEEDED }));
		assertFalse(_cut.isResource(new byte[] { TransientValueDictionary.THRESHOLD_NOT_EXCEEDED, IDictionary.LITERAL_BYTE_FLAG }));
		assertFalse(_cut.isResource(new byte[] { TransientValueDictionary.THRESHOLD_NOT_EXCEEDED, IDictionary.BNODE_BYTE_FLAG }));

		assertTrue(_cut.isResource(new byte[] { TransientValueDictionary.THRESHOLD_NOT_EXCEEDED, IDictionary.RESOURCE_BYTE_FLAG }));
	}

	/**
	 * Long literal dictionary cannot be null.
	 */
	@Test
	public void nullLongLiteralDictionary() {
		try {
			_cut = new TransientValueDictionary(randomString(), null, 0);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing to be done...this is the expected behaviour
		}
	}

	/**
	 * On initialisation, decoratee instance must be initialised too.
	 * 
	 * @throws InitialisationException never, otherwise the test fails.
	 */
	@Test
	public void decorateeInitialisation() throws InitialisationException {
		final ITopLevelDictionary decoratee = mock(ITopLevelDictionary.class);
		
		_cut = new TransientValueDictionary(randomString(), decoratee, TransientValueDictionary.DEFAULT_THRESHOLD);
		_cut.initialise(T_DATA_ACCESS_LAYER_FACTORY);
		
		verify(decoratee).initialise(T_DATA_ACCESS_LAYER_FACTORY);
	}
	
	/**
	 * Remove() method itself has no effect on the {@link TransientValueDictionary} unless the given value is a long literal.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeValueWithoutInvolvingLongLiteralDictionary() throws Exception {
		final URI uri = VALUE_FACTORY.createURI("http://example.org#it");
		final ITopLevelDictionary decoratee = mock(ITopLevelDictionary.class);
		
		_cut = new TransientValueDictionary(randomString(), decoratee, TransientValueDictionary.DEFAULT_THRESHOLD);
		_cut.removeValue(uri, false);
		
		verify(decoratee, times(0)).removeValue(uri, false);
	}
	
	/**
	 * Remove() method itself has no effect on the {@link TransientValueDictionary} unless the given value is a long literal.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeValueInvolvesLongLiteralDictionary() throws Exception {
		final byte[] persistentId = { TransientValueDictionary.THRESHOLD_EXCEEDED, 2, 3, 4, 5, 6, 7, 78, 8, 9 };

		final ITopLevelDictionary decoratee = mock(ITopLevelDictionary.class);
		
		when(decoratee.getID(_longLiteral, false)).thenReturn(persistentId);
		
		_cut = new TransientValueDictionary(randomString(), decoratee, TransientValueDictionary.DEFAULT_THRESHOLD);
		_cut.removeValue(_longLiteral, false);
		
		verify(decoratee).removeValue(_longLiteral, false);		
	}

	/**
	 * Passing 0 as literal threshold length actually disables the wrapped dictionary.
	 */
	@Test
	public void disableLongLiteralDictionary() {
		_cut = new TransientValueDictionary(randomString(), new PersistentValueDictionary(randomString()), 0);
		assertEquals(Integer.MAX_VALUE, _cut._threshold);
		assertTrue(_cut._longLiteralsDictionary instanceof PersistentValueDictionary);		
	}

	/**
	 * Passing -1 (or a negative number) as literal threshold length actually uses the default value.
	 */
	@Test
	public void useDefaultLongLiteralThreshold() {
		final Random random = new Random();
		for (int i = 0; i < random.nextInt(10) + 1; i++) {
			final int negativeThreshold = (random.nextInt(1000) + 1) * -1;
			_cut = new TransientValueDictionary(randomString(), new PersistentValueDictionary(randomString()), negativeThreshold);
			assertEquals(TransientValueDictionary.DEFAULT_THRESHOLD, _cut._threshold);
			assertTrue(_cut._longLiteralsDictionary instanceof PersistentValueDictionary);					
		}
	}

	/**
	 * In case of default constructor, default values are {@link PersistentValueDictionary} for long literals and 1K as threshold.
	 */
	@Test
	public void defaultValues() {
		_cut = new TransientValueDictionary(randomString());
		assertEquals(TransientValueDictionary.DEFAULT_THRESHOLD, _cut._threshold);
		assertTrue(_cut._longLiteralsDictionary instanceof PersistentValueDictionary);
	}
	
	/**
	 * Closing the dictionary will trigger a close() on embedded dictionary.
	 */
	@Test
	public void close() {
		final ITopLevelDictionary dictionary = mock(ITopLevelDictionary.class);

		_cut._longLiteralsDictionary = dictionary;
		_cut.close();

		verify(dictionary).close();
	}

	/**
	 * The value is a literal and its length is over the configured threshold.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIDForLongLiterals() throws Exception {
		assertArrayEquals(_longLiteralId, _cut.getID(_longLiteral, false));
	}
	
	/**
	 * Tests the identifier creation method.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getID() throws Exception {
		assertIdEncodingWithoutThresholdExceeding(
				_firstMember, 
				ValueDictionaryBase.RESOURCE_BYTE_FLAG);
		assertIdEncodingWithoutThresholdExceeding(
				VALUE_FACTORY.createBNode("4356682"), 
				ValueDictionaryBase.BNODE_BYTE_FLAG);
		assertIdEncodingWithoutThresholdExceeding(
				VALUE_FACTORY.createLiteral("This is a literal"), 
				ValueDictionaryBase.LITERAL_BYTE_FLAG);
	}

	/**
	 * If the input value is null then the getID must return null.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIDWithNullValue() throws Exception {	
		assertNull(_cut.getID(null, false));
	}

	/**
	 * Tests the creation of the N3 resource representation.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getN3() throws Exception {
		byte[] firstMemberId = _cut.getID(_firstMember, false);
		assertEquals(_firstMember, _cut.getValue(firstMemberId, false));
	}

	/**
	 * Tests the value retrieval.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValue() throws Exception {
		byte[] firstMemberId = _cut.getID(_firstMember, false);
		assertEquals(_firstMember, _cut.getValue(firstMemberId, false));

		byte[] fourthMemberId = _cut.getID(_fourthMember, false);
		assertEquals(_fourthMember, _cut.getValue(fourthMemberId, false));

		byte[] fifthMemberId = _cut.getID(_fifthMember, false);
		assertTrue(_cut.getValue(fifthMemberId, false) instanceof BNode);
	}

	/**
	 * N3 for long literals.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getN3WithLongLiteral() throws Exception {
		byte[] id = _cut.getID(_longLiteral, false);
		assertArrayEquals(_longLiteralId, id);
		assertEquals(_longLiteral, _cut.getValue(id, false));
	}

	/**
	 * Value is a long literal.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValueWithLongLiteral() throws Exception {
		byte[] id = _cut.getID(_longLiteral, false);
		assertArrayEquals(_longLiteralId, id);
		assertEquals(_longLiteral, _cut.getValue(id, false));
	}
	
	/**
	 * Tests the decomposition of a composite id.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void decomposeWithTwoMembers() throws Exception {
		final byte[] firstId = _cut.getID(_firstMember, false);
		final byte[] secondId = _cut.getID(_secondMember, false);
		
		final byte [] compositeId = _cut.compose(firstId, secondId);
		
		final byte[][] ids = _cut.decompose(compositeId);
		
		assertEquals(2, ids.length);
		assertArrayEquals(firstId, ids[0]);
		assertArrayEquals(secondId, ids[1]);
	}

	/**
	 * Tests the decomposition of a composite id.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void decomposeWithThreeMembers() throws Exception {
		final byte[] firstId = _cut.getID(_firstMember, false);
		final byte[] secondId = _cut.getID(_secondMember, false);
		final byte[] thirdId = _cut.getID(_thirdMember, false);
		
		final byte [] compositeId = _cut.compose(firstId, secondId, thirdId);
		
		final byte[][] ids = _cut.decompose(compositeId);
		
		assertEquals(3, ids.length);
		assertArrayEquals(firstId, ids[0]);
		assertArrayEquals(secondId, ids[1]);
		assertArrayEquals(thirdId, ids[2]);
	}

	/**
	 * No input identifier can be null when creating composite identifiers.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void composeIdWithNullArguments() throws Exception {	
		assertInvalidArgumentsForIdComposition(null, null);
		assertInvalidArgumentsForIdComposition(_cut.getID(_firstMember, false), null);
		assertInvalidArgumentsForIdComposition(null, _cut.getID(_firstMember, false));

		assertInvalidArgumentsForIdComposition(null, null, null);
		assertInvalidArgumentsForIdComposition(_cut.getID(_firstMember, false), null, null);
		assertInvalidArgumentsForIdComposition(null, _cut.getID(_firstMember, false), null);
		assertInvalidArgumentsForIdComposition(null, null, _cut.getID(_firstMember, false));
	}	
		
	/**
	 * Tests the composite identifier build with two members.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void composeIdWithTwoParts() throws Exception {	
		final byte[] firstId = _cut.getID(_firstMember, false);
		final byte[] secondId = _cut.getID(_secondMember, false);
		
		final byte [] compositeId = _cut.compose(firstId, secondId);
		
		int expectedLength = 
				2 					// how many id
				+ 2					// 1st id length
				+ firstId.length	// 1st id
				+ 2					// 2nd id length
				+ secondId.length;	// 2nd id
		assertEquals(expectedLength, compositeId.length);
		
		assertEquals(2, decodeShort(compositeId, 0));

		int offset = 2;
		assertEquals(firstId.length, decodeShort(compositeId, offset));
		
		offset += 2;
		assertArrayEquals(firstId, subarray(compositeId, offset, firstId.length));

		offset += firstId.length;
		assertEquals(secondId.length, decodeShort(compositeId, offset));
		
		offset += 2;		
		assertArrayEquals(secondId, subarray(compositeId, offset, secondId.length));
	}

	/**
	 * Tests the composite identifier build with three members.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void composeIdWithThreeParts() throws Exception {			
		final byte[] firstId = _cut.getID(_firstMember, false);
		final byte[] secondId = _cut.getID(_secondMember, false);
		final byte[] thirdId = _cut.getID(_thirdMember, false);
		
		final byte [] compositeId = _cut.compose(firstId, secondId, thirdId);
		
		int expectedLength = 
				2 					// how many id
				+ 2					// 1st id length
				+ firstId.length	// 1st id
				+ 2					// 2nd id length
				+ secondId.length	// 2nd id
				+ 2					// 3rd id length
				+ thirdId.length;	// 3rd id
		assertEquals(expectedLength, compositeId.length);
		
		assertEquals(3, decodeShort(compositeId, 0));

		int offset = 2;
		assertEquals(firstId.length, decodeShort(compositeId, offset));
		
		offset += 2;
		assertArrayEquals(firstId, subarray(compositeId, offset, firstId.length));

		offset += firstId.length;
		assertEquals(secondId.length, decodeShort(compositeId, offset));
		
		offset += 2;
		assertArrayEquals(secondId, subarray(compositeId, offset, secondId.length));

		offset += secondId.length;
		assertEquals(thirdId.length, decodeShort(compositeId, offset));
		
		offset += 2;
		assertArrayEquals(thirdId, subarray(compositeId, offset, thirdId.length));	
	}
	
	/**
	 * Internal method used for assert identifier encoding over several kind of resources.
	 * Note that this method tests only identifiers generated without involving the internal dictionary 
	 * (the one used for long literals).
	 * 
	 * @param value the value to be encoded.
	 * @param marker a byte that indicates the kind of resource we are encoding.
	 * @throws Exception never otherwise the test fails.
	 */
	private void assertIdEncodingWithoutThresholdExceeding(final Value value, final byte marker) throws Exception {
		final String n3 = NTriplesUtil.toNTriplesString(value);
		final byte[] binary = n3.getBytes(CHARSET_UTF8);
		final byte[] expected = new byte[binary.length + 2];
		expected[0] = TransientValueDictionary.THRESHOLD_NOT_EXCEEDED;
		expected[1] = marker;
		
		fillIn(expected, 2, binary);
		
		assertArrayEquals(expected, _cut.getID(value, false));
	}
	
	/**
	 * Internal method used for asserting a failure in case of invalid input arguments.
	 * 
	 * @param id1 the first identifier.
	 * @param id2 the second idenrtifier.
	 */
	private void assertInvalidArgumentsForIdComposition(final byte[] id1, final byte[] id2) {
		try {
			_cut.compose(id1, id2);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing, this is the expected behaviour
		}
	}
	
	/**
	 * Internal method used for asserting a failure in case of invalid input arguments.
	 * 
	 * @param id1 the first identifier.
	 * @param id2 the second idenrtifier.
	 * @param id3 the third identifier.
	 */
	private void assertInvalidArgumentsForIdComposition(final byte[] id1, final byte[] id2, final byte[] id3) {
		try {
			_cut.compose(id1, id2, id3);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing, this is the expected behaviour
		}
	}	
}