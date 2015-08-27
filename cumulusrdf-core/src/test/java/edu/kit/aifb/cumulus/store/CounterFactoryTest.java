package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.TestUtils.newTripleStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.kit.aifb.cumulus.AbstractCumulusTest;
import edu.kit.aifb.cumulus.store.CounterFactory.Counter;

/**
 * Test case for {@link CounterFactory}.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CounterFactoryTest extends AbstractCumulusTest {

	private class Client extends Thread {

		private Counter _counter;
		private Set<Long> _collected;
		private int _numOfincrements;

		/**
		 * Builds a new thread client.
		 * 
		 * @param counter the instance under test.
		 * @param numOfincrements the number of increments.
		 */
		public Client(final Counter counter, final int numOfincrements) {

			_collected = new HashSet<Long>();
			_counter = counter;
			_numOfincrements = numOfincrements;

			setName("client: " + _counter);
		}

		@Override
		public void run() {

			for (int i = 0; i < _numOfincrements && !isInterrupted(); i++) {

				try {
					_collected.add(_counter.next());
				} catch (Exception exception) {
					throw new RuntimeException(exception);
				}

				try {
					Thread.sleep((long) (Math.random() * 100));
				} catch (InterruptedException e) {
					interrupt();
				}
			}
		}

		public Set<Long> getCollectedValues() {
			return _collected;
		}
	}

	private static CounterFactory _counter_factory;
	private static final List<String> _test_ids = Arrays.asList("c1", "c2", "c3");
	private static final List<Integer> _test_numOfIncrements = Arrays.asList(2,
			10, 50);
	private static final int NUM_OF_CLIENTS = 5;

	@BeforeClass
	public static void beforeAllTests() throws Exception {
		
		_tripleStore = newTripleStore();
		_tripleStore.open();

		_counter_factory = _tripleStore.getCounterFactory();
	}
	
	@Test
	public void createCounterTest() throws Exception {

		for (String id : _test_ids) {
			final Counter c = _counter_factory.getCounter(id);
			assertNotNull(c);
			assertEquals(0l, c.current());
		}
	}

	@Test
	public void updateSingleTest() throws Exception {

		for (int i = 0; i < _test_ids.size(); i++) {

			Counter c = _counter_factory.getCounter(_test_ids.get(i));

			Client client = new Client(c, _test_numOfIncrements.get(i));
			client.start();
			client.join();

			assertEquals((int) _test_numOfIncrements.get(i), client
					.getCollectedValues().size());
			assertEquals((int) _test_numOfIncrements.get(i), c.current());
		}
	}

	@Test
	public void updateMultiTest() throws InterruptedException {

		for (int i = 0; i < _test_ids.size(); i++) {

			Counter c = _counter_factory.getCounter(_test_ids.get(i));
			List<Client> clients = new LinkedList<Client>();

			for (int j = 0; j < NUM_OF_CLIENTS; j++) {
				Client client = new Client(c, _test_numOfIncrements.get(i));
				client.start();
				clients.add(client);
			}

			for (Client client : clients) {
				client.join();
			}

			Set<Long> collectedValues = new HashSet<Long>();

			for (Client client : clients) {
				collectedValues.addAll(client.getCollectedValues());
				assertEquals((int) _test_numOfIncrements.get(i), client
						.getCollectedValues().size());
			}

			assertEquals(_test_numOfIncrements.get(i) * NUM_OF_CLIENTS,
					collectedValues.size());
		}
	}

	@Before
	public void reset() {
		for (String id : _test_ids) {
			_counter_factory.removeCounter(id);
		}
	}
}