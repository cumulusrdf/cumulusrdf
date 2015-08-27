package edu.kit.aifb.cumulus.store.dict.impl.value;

import static edu.kit.aifb.cumulus.TestUtils.RANDOMIZER;
import static edu.kit.aifb.cumulus.TestUtils.T_DATA_ACCESS_LAYER_FACTORY;
import static edu.kit.aifb.cumulus.TestUtils.VALUE_FACTORY;
import static edu.kit.aifb.cumulus.TestUtils.randomString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Value;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.domain.dictionary.IDictionary;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;

/**
 * Test case for {@link ThreeTieredValueDictionary}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class ThreeTieredValueDictionaryTest {

	final Value _aURI = VALUE_FACTORY.createURI("http://pippo.pluto.paperino#first");
	final Value _aLiteral = VALUE_FACTORY.createLiteral("Hello there! It's Gazza!");
	final Value _aBNode = VALUE_FACTORY.createBNode(String.valueOf(System.currentTimeMillis()));
	
	private ThreeTieredValueDictionary _cut;
	private IDictionary<String> _namespacesDictionary;
	private IDictionary<String> _localNamesDictionary;
	private ITopLevelDictionary _bnodesAndLiteralsDictionary;
	
	/**
	 * Setup fixture for this test.
	 * 
	 * @throws Exception never, otherwise the corresponding test will fail.
	 */
	@Before
	@SuppressWarnings("unchecked")
	public void setUp() throws Exception {
		_namespacesDictionary = mock(IDictionary.class);
		_localNamesDictionary = mock(IDictionary.class);
		_bnodesAndLiteralsDictionary = mock(ITopLevelDictionary.class);

		_cut = new ThreeTieredValueDictionary(randomString(), _namespacesDictionary, _localNamesDictionary, _bnodesAndLiteralsDictionary);
	}

	/**
	 * Internally managed dictionaries cannot be null.
	 */
	@Test
	public void nullWrappedDictionary() {
		try {
			_cut = new ThreeTieredValueDictionary(
					randomString(), 
					null, 
					_localNamesDictionary, 
					_bnodesAndLiteralsDictionary);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing, this is the expected behaviour
		}
		
		try {
			_cut = new ThreeTieredValueDictionary(
					randomString(), 
					_namespacesDictionary, 
					null, 
					_bnodesAndLiteralsDictionary);
			fail();
		} catch (final IllegalArgumentException expected) {
			// Nothing, this is the expected behaviour
		}
		
		try {
			_cut = new ThreeTieredValueDictionary(
				randomString(), 
				_namespacesDictionary, 
				_localNamesDictionary, 
				null);		
		} catch (final IllegalArgumentException expected) {
			// Nothing, this is the expected behaviour
		}			
	}
	
	/**
	 * Tests if an identifier is a blank node or not.
	 */
	@Test
	public void isBNode() {
		final byte[] id = new byte[9];
		RANDOMIZER.nextBytes(id);

		assertFalse(_cut.isBNode(null));

		id[0] = ThreeTieredValueDictionary.MARKER;
		assertFalse(_cut.isBNode(id));
		verify(_bnodesAndLiteralsDictionary, times(0)).isBNode(id);

		id[0] = ThreeTieredValueDictionary.MARKER + 1;
		_cut.isBNode(id);

		verify(_bnodesAndLiteralsDictionary).isBNode(id);
	}

	/**
	 * Tests if an identifier is a literal or not.
	 */
	@Test
	public void isLiteral() {
		final byte[] id = new byte[9];
		RANDOMIZER.nextBytes(id);

		assertFalse(_cut.isLiteral(null));

		id[0] = ThreeTieredValueDictionary.MARKER;
		assertFalse(_cut.isLiteral(id));
		verify(_bnodesAndLiteralsDictionary, times(0)).isLiteral(id);

		id[0] = ThreeTieredValueDictionary.MARKER + 1;
		_cut.isLiteral(id);

		verify(_bnodesAndLiteralsDictionary).isLiteral(id);
	}

	/**
	 * Tests if an identifier is a resource or not.
	 */
	@Test
	public void isResource() {
		final byte[] id = new byte[17];
		RANDOMIZER.nextBytes(id);

		assertFalse(_cut.isResource(null));

		id[0] = ThreeTieredValueDictionary.MARKER;
		assertTrue(_cut.isResource(id));

		final byte[] tooShort = new byte[17 - 1];
		final byte[] tooLong = new byte[17 + 1];

		tooShort[0] = ThreeTieredValueDictionary.MARKER;
		tooLong[0] = ThreeTieredValueDictionary.MARKER;

		assertTrue(_cut.isResource(tooShort));
		assertTrue(_cut.isResource(tooLong));

	}
	/**
	 * On initialisation, decoratee instance must be initialised too.
	 * 
	 * @throws InitialisationException never, otherwise the test fails.
	 */
	@Test
	public void decorateeInitialisation() throws InitialisationException {
		_cut.initialise(T_DATA_ACCESS_LAYER_FACTORY);

		verify(_namespacesDictionary).initialise(T_DATA_ACCESS_LAYER_FACTORY);
		verify(_localNamesDictionary).initialise(T_DATA_ACCESS_LAYER_FACTORY);
		verify(_bnodesAndLiteralsDictionary).initialise(T_DATA_ACCESS_LAYER_FACTORY);
	}

	/**
	 * Close must close all managed dictionaries.
	 */
	@Test
	public void close() {
		_cut.close();

		verify(_namespacesDictionary).close();
		verify(_localNamesDictionary).close();
		verify(_bnodesAndLiteralsDictionary).close();
	}

	/**
	 * Removing a URI will remove the corresponding entries on namespaces and local names dictionaries.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeURI() throws Exception {
		final boolean isPredicate = RANDOMIZER.nextBoolean();
		_cut.removeValue(_aURI, isPredicate);

		//		final String namespace = ((URI) _aURI).getNamespace();
		//		final String localName = ((URI) _aURI).getLocalName();

		//		verify(_namespacesDictionary).removeValue(namespace, isPredicate);
		//		verify(_localNamesDictionary, times(0)).removeValue(localName, isPredicate);
		//		verify(_bnodesAndLiteralsDictionary, times(0)).removeValue(_aURI, isPredicate);
	}

	/**
	 * Removing a BNode won't involve namespaces and local names dictionaries.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeBNode() throws Exception {
		final boolean isPredicate = RANDOMIZER.nextBoolean();
		_cut.removeValue(_aBNode, isPredicate);

		verify(_namespacesDictionary, times(0)).removeValue(anyString(), eq(isPredicate));
		verify(_localNamesDictionary, times(0)).removeValue(anyString(), eq(isPredicate));
		verify(_bnodesAndLiteralsDictionary).removeValue(_aBNode, isPredicate);
	}

	/**
	 * Removing a Literal won't involve namespaces and local names dictionaries.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void removeLiteral() throws Exception {
		final boolean isPredicate = RANDOMIZER.nextBoolean();
		_cut.removeValue(_aLiteral, isPredicate);

		verify(_namespacesDictionary, times(0)).removeValue(anyString(), eq(isPredicate));
		verify(_localNamesDictionary, times(0)).removeValue(anyString(), eq(isPredicate));
		verify(_bnodesAndLiteralsDictionary).removeValue(_aLiteral, isPredicate);
	}
}