package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.TestUtils.SELECT_ALL_QUADS_PATTERN;
import static edu.kit.aifb.cumulus.TestUtils.SELECT_ALL_TRIPLES_PATTERN;
import static edu.kit.aifb.cumulus.TestUtils.newQuadStore;
import static edu.kit.aifb.cumulus.TestUtils.newTripleStore;
import static edu.kit.aifb.cumulus.TestUtils.numOfRes;
import static edu.kit.aifb.cumulus.TestUtils.randomStatements;
import static edu.kit.aifb.cumulus.TestUtils.statementIteratorToRdfStream;
import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;

import edu.kit.aifb.cumulus.AbstractCumulusTest;

/**
 * A test case to check if insertion and deletion of iterators with size > batchSize works.
 * 
 * @author Sebastian Schmidt
 * @since 1.1.0
 */
public class LargeModifyStoreTest extends AbstractCumulusTest {

	/**
	 * Test if the quad store works with large iterators.
	 * 
	 * @throws Exception If something bad happens... 
	 */
	@Test
	public void testLargeQuadOperations() throws Exception {
		_quadStore = newQuadStore();
		_quadStore.open();
		try {
			Random random = new Random(4986723946726L);
			List<Statement> statements = randomStatements(random, (int) (1000 * 3.1415));
			InputStream statementStream = statementIteratorToRdfStream(statements.iterator(), RDFFormat.NQUADS);

			_quadStore.bulkLoad(statementStream, RDFFormat.NQUADS);

			assertEquals(
					"Store contains different amount of quads than inserted!",
					statements.size(),
					numOfRes(_quadStore.query(SELECT_ALL_QUADS_PATTERN)));

			_quadStore.removeData(_quadStore.query(SELECT_ALL_QUADS_PATTERN));
			assertEquals(
					"Store should be empty after deleting everything!",
					0,
					numOfRes(_quadStore.query(SELECT_ALL_QUADS_PATTERN)));
		} finally {
			_quadStore.close();
		}
	}

	/**
	 * Test if the triple store works with large iterators.
	 * 
	 * @throws Exception If something bad happens... 
	 */
	@Test
	public void testLargeTripleOperations() throws Exception {
		_tripleStore = newTripleStore();
		_tripleStore.open();

		try {
			Random random = new Random(452662401L);
			List<Statement> statements = randomStatements(random, (int) (1000 * 3.1415));
			InputStream statementStream = statementIteratorToRdfStream(statements.iterator(), RDFFormat.NTRIPLES);

			_tripleStore.bulkLoad(statementStream, RDFFormat.NTRIPLES);

			assertEquals("Store contains different amount of triples than inserted!",
					statements.size(), numOfRes(_tripleStore.query(SELECT_ALL_TRIPLES_PATTERN)));

			_tripleStore.removeData(_tripleStore.query(SELECT_ALL_QUADS_PATTERN));
			assertEquals(
					"Store should be empty after deleting everything!",
					0,
					numOfRes(_tripleStore.query(SELECT_ALL_QUADS_PATTERN)));
		} finally {
			_tripleStore.close();
		}
	}
}