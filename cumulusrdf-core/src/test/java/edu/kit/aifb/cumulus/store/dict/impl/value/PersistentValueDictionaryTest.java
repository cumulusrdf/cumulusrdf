package edu.kit.aifb.cumulus.store.dict.impl.value;

import static edu.kit.aifb.cumulus.TestUtils.RANDOMIZER;
import static edu.kit.aifb.cumulus.TestUtils.randomString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import edu.kit.aifb.cumulus.framework.domain.dictionary.IDictionary;

/**
 * Test case for {@link PersistentValueDictionary}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class PersistentValueDictionaryTest {

	private PersistentValueDictionary _cut;

	/**
	 * Setup fixture for this test case.
	 */
	@Before
	public void setUp() {
		_cut = new PersistentValueDictionary(randomString());
	}

	/**
	 * Tests if an identifier is a blank node or not.
	 */
	@Test
	public void isBNode() {
		byte[] id = new byte[PersistentValueDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		assertFalse(_cut.isBNode(null));
		assertFalse("Wrong id length.", _cut.isBNode(new byte[RANDOMIZER.nextInt(PersistentValueDictionary.ID_LENGTH)]));
		assertFalse("Wrong id length.", _cut.isBNode(new byte[(RANDOMIZER.nextInt(PersistentValueDictionary.ID_LENGTH) + 1) * 100]));

		id[0] = IDictionary.RESOURCE_BYTE_FLAG;
		assertFalse(_cut.isBNode(id));

		id[0] = IDictionary.LITERAL_BYTE_FLAG;
		assertFalse(_cut.isBNode(id));

		id[0] = IDictionary.BNODE_BYTE_FLAG;
		assertTrue(_cut.isBNode(id));
	}

	/**
	 * Tests if an identifier is a literal or not.
	 */
	@Test
	public void isLiteral() {
		byte[] id = new byte[PersistentValueDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		assertFalse(_cut.isBNode(null));
		assertFalse("Wrong id length.", _cut.isLiteral(new byte[RANDOMIZER.nextInt(PersistentValueDictionary.ID_LENGTH)]));
		assertFalse("Wrong id length.", _cut.isLiteral(new byte[(RANDOMIZER.nextInt(PersistentValueDictionary.ID_LENGTH) + 1) * 100]));

		id[0] = IDictionary.RESOURCE_BYTE_FLAG;
		assertFalse(_cut.isLiteral(id));

		id[0] = IDictionary.BNODE_BYTE_FLAG;
		assertFalse(_cut.isLiteral(id));

		id[0] = IDictionary.LITERAL_BYTE_FLAG;
		assertTrue(_cut.isLiteral(id));
	}

	/**
	 * Tests if an identifier is a resource or not.
	 */
	@Test
	public void isResource() {
		byte[] id = new byte[PersistentValueDictionary.ID_LENGTH];
		RANDOMIZER.nextBytes(id);

		assertFalse(_cut.isResource(null));
		assertFalse("Wrong id length.", _cut.isResource(new byte[RANDOMIZER.nextInt(PersistentValueDictionary.ID_LENGTH)]));
		assertFalse("Wrong id length.", _cut.isResource(new byte[(RANDOMIZER.nextInt(PersistentValueDictionary.ID_LENGTH) + 1) * 100]));

		id[0] = IDictionary.LITERAL_BYTE_FLAG;
		assertFalse(_cut.isResource(id));

		id[0] = IDictionary.BNODE_BYTE_FLAG;
		assertFalse(_cut.isResource(id));

		id[0] = IDictionary.RESOURCE_BYTE_FLAG;
		assertTrue(_cut.isResource(id));
	}
}