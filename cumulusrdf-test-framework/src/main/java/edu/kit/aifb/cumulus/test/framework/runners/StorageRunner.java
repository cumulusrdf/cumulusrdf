package edu.kit.aifb.cumulus.test.framework.runners;

/**
 * A storage runner.
 * Since 1.1.0 CumulusRDF supports a so called "modular storage". 
 * In order to make sure that our test suite works with all supported kind of storages, 
 * we need something that is able to start / stop a (most probably) embedded instance of each specific storage.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public interface StorageRunner {
	
	/**
	 * Starts the storage server.
	 * 
	 * @throws Exception in case of failure.
	 */
	void start() throws Exception;

	/**
	 * Stops the storage server.
	 */
	void stop();
}
