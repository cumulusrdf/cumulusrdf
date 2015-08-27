package edu.kit.aifb.cumulus.datasource.impl;

import static edu.kit.aifb.cumulus.datasource.ColumnFamily.O_SPC;
import static edu.kit.aifb.cumulus.datasource.ColumnFamily.PO_SC;
import static edu.kit.aifb.cumulus.datasource.ColumnFamily.S_POC;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import edu.kit.aifb.cumulus.datasource.impl.Cassandra12xTripleIndexDAO;

/**
 * Test case for {@link Cassandra12xTripleIndexDAO}.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class Cassandra12xTripleIndexDAOTest {
	private Cassandra12xTripleIndexDAO _cut;
	
	// Dummy identifiers...we don't need valid id here
	private byte[] _s = { 11, 21, 13, 34, 45 };
	private byte[] _p = { 13, 21, 53, 54, 25 };
	private byte[] _o = { 1, 28, 36, 41, 52 };
	
	private byte[][] _pattern____ = { null, null, null };
	private byte[][] _pattern_spo = { _s, _p, _o };
	private byte[][] _pattern_sp_ = { _s, _p, null };
	private byte[][] _pattern__po = { null, _p, _o };
	private byte[][] _pattern_s_o = { _s, null, _o };
	private byte[][] _pattern__p_ = { null, _p, null };
	private byte[][] _pattern_s__ = { _s, null, null };
	private byte[][] _pattern___o = { null, null, _o };
	
	/**
	 * Setup fixture for this test case.
	 */
	@Before
	public void setUp() {
		_cut = new Cassandra12xTripleIndexDAO(null, null);
	}
	
	/**
	 * Tests the association between a given triple pattern and the column family involved.
	 */
	@Test
	public void columnFamily() {
		assertEquals(S_POC, _cut.tripleStoreColumnFamily(_pattern____));
		assertEquals(S_POC, _cut.tripleStoreColumnFamily(_pattern_sp_));
		assertEquals(PO_SC, _cut.tripleStoreColumnFamily(_pattern__po));
		assertEquals(O_SPC, _cut.tripleStoreColumnFamily(_pattern_s_o));
		assertEquals(PO_SC, _cut.tripleStoreColumnFamily(_pattern__p_));
		assertEquals(S_POC, _cut.tripleStoreColumnFamily(_pattern_s__));
		assertEquals(O_SPC, _cut.tripleStoreColumnFamily(_pattern___o));
		assertEquals(O_SPC, _cut.tripleStoreColumnFamily(_pattern_spo));
	}
}
