package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.TestUtils.randomString;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests case for TripleStore.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class StoreUnitTest {

	private Store _cut;
	
	/**
	 * Store must have an identity, supplied at construction time.
	 */
	@Test
	public void setId() {
		String id = randomString();
		_cut = new TripleStore(id);

		assertEquals(id, _cut.getId());

		id = randomString();
		_cut = new QuadStore(id);

		assertEquals(id, _cut.getId());
	}
}
