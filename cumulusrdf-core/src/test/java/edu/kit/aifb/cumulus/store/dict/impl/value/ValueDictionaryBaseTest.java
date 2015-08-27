package edu.kit.aifb.cumulus.store.dict.impl.value;

import static edu.kit.aifb.cumulus.TestUtils.RANDOMIZER;
import static edu.kit.aifb.cumulus.TestUtils.buildResource;
import static edu.kit.aifb.cumulus.TestUtils.randomString;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.model.Value;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.domain.dictionary.DictionaryRuntimeContext;

/**
 * Test case for {@link ValueDictionaryBase}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class ValueDictionaryBaseTest {

	private ValueDictionaryBase _cut;

	/**
	 * Setup fixture for this test case.
	 */
	@Before
	public void setUp() {
		_cut = spy(new ValueDictionaryBase(randomString()) {

			@Override
			public void initialiseInternal(final DataAccessLayerFactory factory) throws InitialisationException {
				// Nothing
			}

			@Override
			public Value getValueInternal(final byte[] id, final boolean p) {
				return null;
			}

			@Override
			public byte[] getIdInternal(final Value node, final boolean p) {
				return null;
			}

			@Override
			public void closeInternal() {
				// Nothing
			}

			@Override
			public void removeValue(final Value value, final boolean p) {
				// Nothing
			}

			@Override
			public byte[][] decompose(final byte[] compositeId) {
				return null;
			}

			@Override
			public byte[] compose(final byte[] id1, final byte[] id2, final byte[] id3) {
				return null;
			}

			@Override
			public byte[] compose(final byte[] id1, final byte[] id2) {
				return null;
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
		});
	}

	/**
	 * The dictionary must own a dictionary runtime context.
	 */
	@Test
	public void dictionaryRuntimeContext() {
		final DictionaryRuntimeContext context = ValueDictionaryBase.RUNTIME_CONTEXTS.get();

		final DictionaryRuntimeContext mustBeTheSame = ValueDictionaryBase.RUNTIME_CONTEXTS.get();

		assertSame(context, mustBeTheSame);
	}

	/**
	 * getIDs must forward to the getID concrete implementation.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getTripleIDs() throws Exception {
		final Value s = buildResource(randomString());
		final Value p = buildResource(randomString());
		final Value o = buildResource(randomString());

		_cut.getIDs(s, p, o);
		
		verify(_cut).getID(s, false);
		verify(_cut).getID(p, true);
		verify(_cut).getID(o, false);
	}

	/**
	 * getIDs must forward to the getID concrete implementation.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Test
	public void getQuadIDs() throws Exception {
		final Value s = buildResource(randomString());
		final Value p = buildResource(randomString());
		final Value o = buildResource(randomString());
		final Value c = buildResource(randomString());

		_cut.getIDs(s, p, o, c);

		verify(_cut).getID(s, false);
		verify(_cut).getID(p, true);
		verify(_cut).getID(o, false);
		verify(_cut).getID(c, false);
	}
	
	/**
	 * getValues must forward to the getValue concrete implementation.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Ignore
	@Test
	public void getTripleStatement() throws Exception {
		byte[] s = new byte[RANDOMIZER.nextInt(25)];
		byte[] p = new byte[RANDOMIZER.nextInt(25)];
		byte[] o = new byte[RANDOMIZER.nextInt(25)];

		RANDOMIZER.nextBytes(s);
		RANDOMIZER.nextBytes(p);
		RANDOMIZER.nextBytes(o);

		_cut.getValues(s, p, o);

		verify(_cut).getValue(s, false);
		verify(_cut).getValue(p, true);
		verify(_cut).getValue(o, false);
	}

	/**
	 * getValues must forward to the getValue concrete implementation.
	 * 
	 * @throws Exception never otherwise the test fails.
	 */
	@Ignore
	@Test
	public void getQuadStatement() throws Exception {
		byte[] s = new byte[RANDOMIZER.nextInt(25)];
		byte[] p = new byte[RANDOMIZER.nextInt(25)];
		byte[] o = new byte[RANDOMIZER.nextInt(25)];
		byte[] c = new byte[RANDOMIZER.nextInt(25)];

		RANDOMIZER.nextBytes(s);
		RANDOMIZER.nextBytes(p);
		RANDOMIZER.nextBytes(o);
		RANDOMIZER.nextBytes(c);

		_cut.getValues(s, p, o, c);

		verify(_cut).getValue(s, false);
		verify(_cut).getValue(p, true);
		verify(_cut).getValue(o, false);
		verify(_cut).getValue(c, false);
	}
}