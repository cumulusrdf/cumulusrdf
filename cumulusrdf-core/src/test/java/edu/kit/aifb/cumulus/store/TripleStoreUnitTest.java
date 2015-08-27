package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.TestUtils.newTripleStore;
import static edu.kit.aifb.cumulus.TestUtils.randomInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests case for TripleStore.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class TripleStoreUnitTest {

	private TripleStore _cut;

	/**
	 * Setup fixture for this test case.
	 */
	@Before
	public void setUp() {
		_cut = (TripleStore) newTripleStore();
	}
	
	/**
	 * If injected batch limit is lesser than 0, then it must be ignored.
	 */
	@Test
	public void setInvalidBatchLimit() {
		assertTrue(_cut._batchLimit > 0);
		
		final int previousBatchLimit = _cut._batchLimit;
		
		_cut.setDefaultBatchLimit(0);
		assertEquals(previousBatchLimit, _cut._batchLimit);
		
		int badValue = randomInt() + 1;
		badValue = badValue < 0 ? badValue : badValue * -1;
		_cut.setDefaultBatchLimit(badValue);
		assertEquals(previousBatchLimit, _cut._batchLimit);
	}
	
	/**
	 * If injected batch limit is greater than 0, then it must be used.
	 */
	@Test
	public void setBatchLimit() {
		assertTrue(_cut._batchLimit > 0);
		
		int value = randomInt() + 1;
		value = value < 0 ? value * -1 : value;
		_cut.setDefaultBatchLimit(value);
		assertEquals(value, _cut._batchLimit);
	}	

	/**
	 * Range indexes can be enabled only before opening the store.
	 */
	public void enableRangeIndexesBeforeOpeningTheStore() {
		assertFalse(_cut.isRangeIndexesSupportEnabled());

		_cut.enableRangeIndexesSupport();

		assertTrue(_cut.isRangeIndexesSupportEnabled());
	}

	/**
	 * Enabling range indexes after opening the store has no effect.
	 */
	public void enableRangeIndexesAfterOpeningTheStore() {
		// FIXME Do not use default visibility just for this...
		_cut._isOpen = true;
		assertTrue(_cut.isOpen());

		assertFalse(_cut.isRangeIndexesSupportEnabled());

		_cut.enableRangeIndexesSupport();

		assertFalse(_cut.isRangeIndexesSupportEnabled());
	}
}
