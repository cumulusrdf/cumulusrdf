package edu.kit.aifb.cumulus;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.store.BIndexTest;
import edu.kit.aifb.cumulus.store.CounterFactoryTest;
import edu.kit.aifb.cumulus.store.DeletionTest;
import edu.kit.aifb.cumulus.store.LargeModifyStoreTest;
import edu.kit.aifb.cumulus.store.PersistentMapTest;
import edu.kit.aifb.cumulus.store.PersistentSetTest;
import edu.kit.aifb.cumulus.store.QuadsDeletionTest;
import edu.kit.aifb.cumulus.store.QuadsTest;
import edu.kit.aifb.cumulus.store.QueryTest;
import edu.kit.aifb.cumulus.store.RangeQueriesTest;
import edu.kit.aifb.cumulus.store.RangeQueryOptimizationTest;
import edu.kit.aifb.cumulus.store.RemoveTest;
import edu.kit.aifb.cumulus.store.StoreUnitTest;
import edu.kit.aifb.cumulus.store.TripleStoreUnitTest;
import edu.kit.aifb.cumulus.store.dict.impl.string.CacheStringDictionaryTest;
import edu.kit.aifb.cumulus.store.dict.impl.string.PersistentStringDictionaryTest;
import edu.kit.aifb.cumulus.store.dict.impl.string.StringDictionaryBaseTest;
import edu.kit.aifb.cumulus.store.dict.impl.value.CacheModeTest;
import edu.kit.aifb.cumulus.store.dict.impl.value.CacheValueDictionaryTest;
import edu.kit.aifb.cumulus.store.dict.impl.value.KnownURIsDictionaryTest;
import edu.kit.aifb.cumulus.store.dict.impl.value.PersistentValueDictionaryTest;
import edu.kit.aifb.cumulus.store.dict.impl.value.ThreeTieredValueDictionaryTest;
import edu.kit.aifb.cumulus.store.dict.impl.value.TransientValueDictionaryTest;
import edu.kit.aifb.cumulus.store.dict.impl.value.ValueDictionaryBaseTest;
import edu.kit.aifb.cumulus.store.sesame.SesameSailQuadsTest;
import edu.kit.aifb.cumulus.store.sesame.SesameSailSparqlTest;
import edu.kit.aifb.cumulus.test.framework.runners.StorageRunner;
import edu.kit.aifb.cumulus.util.UtilTest;

/**
 * CumulusRDF Test suite.
 * This class has been introduced in version 1.1.0 in order to have a common, unique, central point for starting / stopping 
 * the specific (embedded) storage we are going to use in our test cases.
 *  
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
@RunWith(Suite.class)
@SuiteClasses({
		DeletionTest.class,
		LargeModifyStoreTest.class,
		UtilTest.class,
		BIndexTest.class,
		CounterFactoryTest.class,
		PersistentMapTest.class,
		PersistentSetTest.class,
		QuadsDeletionTest.class,
		QuadsTest.class,
		QueryTest.class,
		RangeQueriesTest.class,
		RangeQueryOptimizationTest.class,
		RemoveTest.class,
		StoreUnitTest.class,
		TripleStoreUnitTest.class,
		CacheStringDictionaryTest.class,
		PersistentStringDictionaryTest.class,
		StringDictionaryBaseTest.class,
		CacheModeTest.class,
		CacheValueDictionaryTest.class,
		KnownURIsDictionaryTest.class,
		PersistentValueDictionaryTest.class,
		ThreeTieredValueDictionaryTest.class,
		TransientValueDictionaryTest.class,
		ValueDictionaryBaseTest.class,
		SesameSailQuadsTest.class,
		SesameSailSparqlTest.class
})
public final class CumulusTestSuite {

	private static Log _log = new Log(LoggerFactory.getLogger(CumulusTestSuite.class));
	private static StorageRunner _runner;

	/**
	 * Starts the external storage before running the test suite.
	 * 
	 * @throws Exception never otherwise the test suite cannot be started.
	 */
	@BeforeClass
	public static void setUp() throws Exception {
		
		final String runnerClass = System.getProperty("storage.runner");
		
		try {
			
			_runner = (StorageRunner) Class.forName(runnerClass).newInstance();

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					try {
						_runner.stop();
					} catch (final Exception ignore) {
						// Nothing, probably Cassandra has been already shut down.
					}
				}
			});

			_runner.start();
			
		} catch (final Exception exception) {			
			_log.error("Unable to instantiate runner: "+ runnerClass, exception);			
			throw new IllegalArgumentException("Unable to instantiate runner " + runnerClass
					+ ". You should indicate a valid profile in your maven build, which should in turn contain a valid runner.");
		}
	}

	/**
	 * Stops the external storage before running the test suite.
	 */
	@AfterClass
	public static void tearDown() {
		try {
			if (_runner != null) {
				_runner.stop();
			}
		} catch (final Exception exception) {
			// Nothing to be done here...
		}
	}
	
	/**
	 * Utility class.
	 */
	private CumulusTestSuite() {
	}
}