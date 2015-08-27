package edu.kit.aifb.cumulus;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import edu.kit.aifb.cumulus.test.framework.runners.StorageRunner;
import edu.kit.aifb.cumulus.webapp.CRUDServletDeleteTest;
import edu.kit.aifb.cumulus.webapp.CRUDServletGetTest;
import edu.kit.aifb.cumulus.webapp.CRUDServletPostTest;
import edu.kit.aifb.cumulus.webapp.CRUDServletPutTest;
import edu.kit.aifb.cumulus.webapp.SPARQLServletTest2;
import edu.kit.aifb.cumulus.webapp.LinkedDataServletTest;
import edu.kit.aifb.cumulus.webapp.SPARQLServletTest;
import edu.kit.aifb.cumulus.webapp.SesameHTTPRepositoryTest;

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
		CRUDServletDeleteTest.class,
		CRUDServletGetTest.class,
		CRUDServletPostTest.class,
		CRUDServletPutTest.class,
		SPARQLServletTest2.class,
		LinkedDataServletTest.class,
		SesameHTTPRepositoryTest.class,
		SPARQLServletTest.class })
		
public class CumulusWebTestSuite {

	private static StorageRunner RUNNER;

	/**
	 * Starts the external storage before running the test suite.
	 * 
	 * @throws Exception never otherwise the test suite cannot be started.
	 */
	@BeforeClass
	public static void setUp() throws Exception {

		final String runnerClass = System.getProperty("storage.runner");

		try {

			RUNNER = (StorageRunner) Class.forName(runnerClass).newInstance();

			Runtime.getRuntime().addShutdownHook(new Thread() {

				@Override
				public void run() {
					try {
						RUNNER.stop();
					} catch (final Exception ignore) {
						// Nothing, probably Cassandra has been already shut down.
					}
				}
			});

			RUNNER.start();

		} catch (final Exception exception) {
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
			if (RUNNER != null) {
				RUNNER.stop();
			}
		} catch (final Exception exception) {
			// Nothing to be done here...
		}
	}
}
