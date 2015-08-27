package edu.kit.aifb.cumulus.store.dict.impl.value;

import static edu.kit.aifb.cumulus.TestUtils.RANDOMIZER;
import static edu.kit.aifb.cumulus.TestUtils.T_DATA_ACCESS_LAYER_FACTORY;
import static edu.kit.aifb.cumulus.TestUtils.VALUE_FACTORY;
import static edu.kit.aifb.cumulus.TestUtils.buildBNode;
import static edu.kit.aifb.cumulus.TestUtils.buildLiteral;
import static edu.kit.aifb.cumulus.TestUtils.buildResource;
import static edu.kit.aifb.cumulus.TestUtils.randomString;
import static edu.kit.aifb.cumulus.framework.util.Bytes.decodeShort;
import static edu.kit.aifb.cumulus.framework.util.Bytes.subarray;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.SKOS;
import org.openrdf.rio.ntriples.NTriplesUtil;

import edu.kit.aifb.cumulus.framework.domain.dictionary.IDictionary;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.store.BIndex;

/**
 * Test case for {@link KnownURIsDictionary}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class KnownURIsDictionaryTest {

	private KnownURIsDictionary _cut;
	private ITopLevelDictionary _decoratee;
	private BIndex _dummyIndex;
	private boolean _isPredicate;

	private final Value _firstMember = VALUE_FACTORY.createURI("http://pippo.pluto.paperino#first");

	/**
	 * Setup fixture for this test case.
	 * 
	 * @throws Exception never, otherwise the corresponding test fails.
	 */
	@Before
	public void setUp() throws Exception {
		_isPredicate = RANDOMIZER.nextBoolean();

		_dummyIndex = mock(BIndex.class);

		_decoratee = mock(ITopLevelDictionary.class);
		_cut = new KnownURIsDictionary(randomString(), _decoratee) {

			@Override
			protected BIndex createIndex() {
				return _dummyIndex;
			}
		};

		_cut.initialise(T_DATA_ACCESS_LAYER_FACTORY);
		verify(_decoratee).initialise(T_DATA_ACCESS_LAYER_FACTORY);
	}

	/**
	 * Using zero-args constructor or specifying null / zero domains or as input parameter means "use default predefined domains".
	 */
	@Test
	public void useDefaultDomains() {
		assertArrayEquals(KnownURIsDictionary.DEFAULT_DOMAINS, _cut._domains);

		final String[][] noDomains = { null, new String[] {} };
		for (String[] domains : noDomains) {
			_cut = new KnownURIsDictionary(randomString(), _decoratee, domains);
			assertArrayEquals(KnownURIsDictionary.DEFAULT_DOMAINS, _cut._domains);
		}
	}

	/**
	 * Using the appropriate constructor, it is possible to define a custom set of domains.
	 */
	@Test
	public void useInjectedDomains() {
		final String[] customDomains = { FOAF.NAMESPACE, SKOS.NAMESPACE };

		_cut = new KnownURIsDictionary(randomString(), _decoratee, customDomains);

		assertArrayEquals(customDomains, _cut._domains);
	}

	/**
	 * A null decoratee will raise an exception when the dictionary is intialized.
	 */
	@Test
	public void nullDecoratee() {
		try {
			_cut = new KnownURIsDictionary(randomString(), null);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing. This is the expected behaviour.
		}

		try {
			_cut = new KnownURIsDictionary(randomString(), null, FOAF.NAMESPACE);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing. This is the expected behaviour.
		}
	}

	/**
	 * A request is received for a literal or a bnode. 
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIdWithIgnorableValue() throws Exception {
		final Value[] ignorableValues = { buildLiteral(randomString()), buildBNode(randomString()) };

		for (final Value value : ignorableValues) {
			_cut.getID(value, _isPredicate);
			verify(_decoratee).getID(value, _isPredicate);
		}
	}

	/**
	 * A request is received for a URI with a namespace not belonging to managed domains. 
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIdWithIgnorableURI() throws Exception {
		final String[] ignorableNamespaces = {
				"http://pippo.pluto.paperino/",
				"http://this.should.not.be.already.managed",
				"http://also.this.shouldnt"
		};

		for (final String ignorableNamespace : ignorableNamespaces) {
			assertFalse(_cut.contains(ignorableNamespace));

			final Value ignorableURI = buildResource(ignorableNamespace + randomString());

			_cut.getID(ignorableURI, _isPredicate);
			verify(_decoratee).getID(ignorableURI, _isPredicate);
		}
	}

	/**
	 * A request is received for a URI with a namespace that belongs to managed domains. 
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIdWithManagedURI() throws Exception {
		final String[] managedNamespaces = {
				FOAF.NAMESPACE,
				RDFS.NAMESPACE,
				OWL.NAMESPACE
		};

		for (final String managedNamespace : managedNamespaces) {
			assertTrue(_cut.contains(managedNamespace));

			final Value uri = buildResource(managedNamespace + randomString());

			final String n3 = NTriplesUtil.toNTriplesString(uri);

			// Make sure the mock index returns "Sorry, we don't have such value".
			when(_dummyIndex.get(n3)).thenReturn(ValueDictionaryBase.NOT_SET);

			// 1. ask for uri.
			byte[] id = _cut.getID(uri, _isPredicate);

			// 2. make sure the identifier is well-formed.
			assertEquals(KnownURIsDictionary.ID_LENGTH, id.length);
			assertEquals(KnownURIsDictionary.KNOWN_URI_MARKER, id[0]);
			assertEquals(ValueDictionaryBase.RESOURCE_BYTE_FLAG, id[1]);

			// 3. make sure the decoratee wasn't involved in identifier creation.
			verify(_decoratee, times(0)).getID(uri, _isPredicate);
			verify(_dummyIndex).putQuick(n3, id);

			reset(_decoratee, _dummyIndex);
		}
	}

	/**
	 * Identifiers for bnodes and literals must be handled by the decoratee.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValueWithIgnorableId() throws Exception {
		final int length = RANDOMIZER.nextInt(KnownURIsDictionary.ID_LENGTH) + 10;
		byte[] ignorableId = new byte[length];

		RANDOMIZER.nextBytes(ignorableId);

		_cut.getValue(ignorableId, _isPredicate);
		verify(_decoratee).getValue(ignorableId, _isPredicate);
	}

	/**
	 * If input identifier is null then result is null.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getValueWithNullInput() throws Exception {
		assertNull(_cut.getValue(null, _isPredicate));
	}

	/**
	 * If input value is null then identifier is null.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getIdWithNullInput() throws Exception {
		assertNull(_cut.getID((Value) null, _isPredicate));
	}

	/**
	 * Identifiers for managed URI must be be directly served by the dictionary, without involving the decoratee.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getURIWithManagedId() throws Exception {
		final URI managedUri = buildResource(FOAF.NAMESPACE + System.currentTimeMillis());

		final String n3 = NTriplesUtil.toNTriplesString(managedUri);

		when(_dummyIndex.getQuick(any(byte[].class))).thenReturn(n3);
		when(_dummyIndex.get(n3)).thenReturn(ValueDictionaryBase.NOT_SET);

		byte[] id = _cut.getID(managedUri, _isPredicate);

		final Value value = _cut.getValue(id, _isPredicate);
		assertEquals(managedUri, value);

		verify(_decoratee, times(0)).getValue(id, _isPredicate);
	}

	/**
	 * A remove on an ignorable value must call the remove on the decoratee.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeIgnorableValue() throws Exception {
		final Value[] ignorableValues = { buildLiteral(randomString()), buildBNode(randomString()) };

		for (final Value value : ignorableValues) {
			_cut.removeValue(value, _isPredicate);
			verify(_decoratee).removeValue(value, _isPredicate);
		}
	}

	/**
	 * A remove on an ignorable URI must call the remove on the decoratee.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeIgnorableURI() throws Exception {
		final String[] ignorableNamespaces = {
				"http://pippo.pluto.paperino/",
				"http://this.should.not.be.already.managed",
				"http://also.this.shouldnt"
		};

		for (final String ignorableNamespace : ignorableNamespaces) {
			assertFalse(_cut.contains(ignorableNamespace));

			final Value ignorableURI = buildResource(ignorableNamespace + randomString());

			_cut.removeValue(ignorableURI, _isPredicate);
			verify(_decoratee).removeValue(ignorableURI, _isPredicate);
		}
	}

	/**
	 * Tests remove() method with managed URI.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeManagedURI() throws Exception {
		final String[] managedNamespaces = {
				FOAF.NAMESPACE,
				RDFS.NAMESPACE,
				OWL.NAMESPACE };

		for (final String managedNamespace : managedNamespaces) {
			assertTrue(_cut.contains(managedNamespace));

			final Value uri = buildResource(managedNamespace + randomString());
			final String n3 = NTriplesUtil.toNTriplesString(uri);

			_cut.removeValue(uri, _isPredicate);
			verify(_dummyIndex).remove(n3);

			reset(_dummyIndex);
		}
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
	 * Tests the composite identifier build with three members.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void composeIdWithThreeParts() throws Exception {
		final byte[] firstId = { 1, 2, 3, 4, 5, 6, 7 };
		final byte[] secondId = { 11, 21, 31, 41, 51, 61, 71 };
		final byte[] thirdId = { 12, 22, 32, 42, 52, 62, 72, 82 };

		final byte[] compositeId = _cut.compose(firstId, secondId, thirdId);

		int expectedLength =
				2 // how many id
						+ 2 // 1st id length
						+ firstId.length // 1st id
						+ 2 // 2nd id length
						+ secondId.length // 2nd id
						+ 2 // 3rd id length
						+ thirdId.length; // 3rd id
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
	 * Tests the composite identifier build with two members.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void composeIdWithTwoParts() throws Exception {
		final byte[] firstId = { 1, 2, 3, 4, 5, 6, 7 };
		final byte[] secondId = { 11, 21, 31, 41, 51, 61, 71 };

		final byte[] compositeId = _cut.compose(firstId, secondId);

		int expectedLength =
				2 // how many id
						+ 2 // 1st id length
						+ firstId.length // 1st id
						+ 2 // 2nd id length
						+ secondId.length; // 2nd id
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
	 * Tests the decomposition of a composite id.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void decomposeWithThreeMembers() throws Exception {
		final byte[] firstId = { 1, 2, 3, 4 };
		final byte[] secondId = { 5, 6, 7, 8 };
		final byte[] thirdId = { 9, 10, 11, 12 };

		final byte[] compositeId = _cut.compose(firstId, secondId, thirdId);

		final byte[][] ids = _cut.decompose(compositeId);

		assertEquals(3, ids.length);
		assertArrayEquals(firstId, ids[0]);
		assertArrayEquals(secondId, ids[1]);
		assertArrayEquals(thirdId, ids[2]);
	}

	/**
	 * Tests the decomposition of a composite id.
	 * 
	 * @throws Exception never, otherwise the test fails.
	 */
	@Test
	public void decomposeWithTwoMembers() throws Exception {
		final byte[] firstId = { 1, 2, 3, 4, 5 };
		final byte[] secondId = { 6, 5, 4, 3, 2, 1 };

		final byte[] compositeId = _cut.compose(firstId, secondId);

		final byte[][] ids = _cut.decompose(compositeId);

		assertEquals(2, ids.length);
		assertArrayEquals(firstId, ids[0]);
		assertArrayEquals(secondId, ids[1]);
	}

	/**
	 * Tests if an identifier is a blank node or not.
	 */
	@Test
	public void isBNode() {
		final byte[] id = new byte[KnownURIsDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		assertFalse(_cut.isBNode(null));

		id[0] = KnownURIsDictionary.KNOWN_URI_MARKER;
		assertFalse(_cut.isBNode(id));

		id[0] = KnownURIsDictionary.KNOWN_URI_MARKER + 1;
		_cut.isBNode(id);

		verify(_decoratee).isBNode(id);
	}

	/**
	 * Tests if an identifier is a literal or not.
	 */
	@Test
	public void isLiteral() {
		final byte[] id = new byte[KnownURIsDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		assertFalse(_cut.isLiteral(null));

		id[0] = KnownURIsDictionary.KNOWN_URI_MARKER;
		assertFalse(_cut.isLiteral(id));

		id[0] = KnownURIsDictionary.KNOWN_URI_MARKER + 1;
		_cut.isLiteral(id);

		verify(_decoratee).isLiteral(id);
	}

	/**
	 * Tests if an identifier is a resource or not.
	 */
	@Test
	public void isResource() {
		final byte[] id = new byte[KnownURIsDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		assertFalse(_cut.isResource(null));

		id[0] = KnownURIsDictionary.KNOWN_URI_MARKER + 1;
		_cut.isResource(id);
		verify(_decoratee).isResource(id);
		reset(_decoratee);

		id[1] = IDictionary.RESOURCE_BYTE_FLAG + 1;
		_cut.isResource(id);
		verify(_decoratee).isResource(id);
		reset(_decoratee);

		final byte[] tooShort = new byte[KnownURIsDictionary.ID_LENGTH - 1];
		final byte[] tooLong = new byte[KnownURIsDictionary.ID_LENGTH + 1];

		tooShort[0] = KnownURIsDictionary.KNOWN_URI_MARKER;
		tooLong[0] = KnownURIsDictionary.KNOWN_URI_MARKER;
		tooShort[1] = IDictionary.RESOURCE_BYTE_FLAG;
		tooLong[1] = IDictionary.RESOURCE_BYTE_FLAG;

		_cut.isResource(tooShort);
		verify(_decoratee).isResource(tooShort);
		reset(_decoratee);

		_cut.isResource(tooLong);
		verify(_decoratee).isResource(tooLong);
		reset(_decoratee);

		id[0] = KnownURIsDictionary.KNOWN_URI_MARKER;
		id[1] = IDictionary.RESOURCE_BYTE_FLAG;

		assertTrue(_cut.isResource(id));
		verify(_decoratee, times(0)).isResource(id);
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