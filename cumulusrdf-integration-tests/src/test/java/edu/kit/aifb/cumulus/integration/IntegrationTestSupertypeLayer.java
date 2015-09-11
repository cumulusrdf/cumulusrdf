/**
 * This is part of the Integration Test suite developed as part of the SolRDF [1] project.
 * 
 * [1] https://github.com/agazzarini/SolRDF
 */
package edu.kit.aifb.cumulus.integration;

import static edu.kit.aifb.cumulus.test.TestUtility.DUMMY_BASE_URI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResults;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.memory.MemoryStore;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.store.TripleStore;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFSail;
import edu.kit.aifb.cumulus.test.MisteryGuest;

/**
 * Supertype layer for all integration tests.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class IntegrationTestSupertypeLayer {
	protected final Log log = new Log(LoggerFactory.getLogger(getClass()));

	protected static Repository inMemoryRepository;
	protected static Repository cumulusRepository;
	
	protected static RepositoryConnection localConnection;
	protected static RepositoryConnection cumulusConnection;
	
	/**
	 * Setup fixture for this test.
	 */
	@BeforeClass
	public static void init() throws Exception {
		inMemoryRepository = new SailRepository(new MemoryStore());
		cumulusRepository = new SailRepository(new CumulusRDFSail(new TripleStore()));
		
		inMemoryRepository.initialize();
		cumulusRepository.initialize();
		
		localConnection = inMemoryRepository.getConnection();
		cumulusConnection = cumulusRepository.getConnection();
	}
	 
	/** 
	 * Shutdown procedure for this test.
	 * 
	 * @throws Exception hopefully never.
	 */
	@AfterClass
	public static void shutdown() throws Exception {		
		try { cumulusConnection.close(); } catch (final Exception ignore) {}
		try { localConnection.close(); } catch (final Exception ignore) {}
		
		try { cumulusRepository.shutDown(); } catch (final Exception ignore) {}
		try { inMemoryRepository.shutDown(); } catch (final Exception ignore) {}
	}		
	
	/**
	 * Removes all data created by this test.
	 * 
	 * @throws Exception hopefully never.
	 */
	protected void clearDatasets() throws Exception {
		localConnection.clear();
		cumulusConnection.clear();
	}
	
	/**
	 * Reads a query from the file associated with this test and builds a query string.
	 * 
	 * @param filename the filename.
	 * @return the query string associated with this test.
	 * @throws IOException in case of I/O failure while reading the file.
	 */
	protected String queryString(final String filename) throws IOException {
		return readFile(filename);
	}
	
	/**
	 * Builds a string from a given file.
	 * 
	 * @param filename the filename (without path).
	 * @return a string with the file content.
	 * @throws IOException in case of I/O failure while reading the file.
	 */
	protected String readFile(final String filename) throws IOException {
		return new String(Files.readAllBytes(Paths.get(source(filename).toURI())));
	}	
	
	/**
	 * Returns the URI of a given filename.
	 * 
	 * @param filename the filename.
	 * @return the URI (as string) of a given filename.
	 */ 
	protected File source(final String filename) {
		return new File(examplesDirectory(), filename);
	}	

	/**
	 * Executes a given ASK query against a given dataset.
	 * 
	 * @param data the mistery guest containing test data (query and dataset)
	 * @throws Exception never, otherwise the test fails.
	 */
	protected void askTest(final MisteryGuest data) throws Exception {
		load(data);
		
		final String queryString = queryString(data.query);
		final BooleanQuery localQuery = localConnection.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
		final BooleanQuery cumulusQuery = cumulusConnection.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
		
		assertEquals(localQuery.evaluate(), cumulusQuery.evaluate());
	}
		
	/**
	 * Executes a given CONSTRUCT query against a given dataset.
	 * 
	 * @param data the mistery guest containing test data (query and dataset)
	 * @throws Exception never, otherwise the test fails.
	 */
	protected void describeTest(final MisteryGuest data) throws Exception {
		constructTest(data);
	}	
	
	/**
	 * Executes a given CONSTRUCT query against a given dataset.
	 * 
	 * @param data the mistery guest containing test data (query and dataset)
	 * @throws Exception never, otherwise the test fails.
	 */
	protected void constructTest(final MisteryGuest data) throws Exception {
		load(data);
		
		final String queryString = queryString(data.query);
		final GraphQuery localQuery = localConnection.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
		final GraphQuery cumulusQuery = cumulusConnection.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
		
		GraphQueryResult localResult = localQuery.evaluate();
		GraphQueryResult cumulusResult = cumulusQuery.evaluate();
		
		assertTrue(QueryResults.equals(localResult, cumulusResult));
		
		cumulusResult.close();
		localResult.close();
	}
	
	/**
	 * Executes a given SELECT query against a given dataset.
	 * 
	 * @param data the mistery guest containing test data (query and dataset)
	 * @throws Exception never, otherwise the test fails.
	 */
	protected void selectTest(final MisteryGuest data) throws Exception {
		load(data);
		
		final String queryString = queryString(data.query);
		final TupleQuery localQuery = localConnection.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		final TupleQuery cumulusQuery = cumulusConnection.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		
		TupleQueryResult localResult = localQuery.evaluate();
		TupleQueryResult cumulusResult = cumulusQuery.evaluate();
		
		assertTrue(QueryResults.equals(localResult, cumulusResult));
		
		cumulusResult.close();
		localResult.close();
	}
	
	/**
	 * Loads all triples found in the datafile associated with the given name.
	 * 
	 * @param datafileName the name of the datafile.
	 * @param graphs an optional set of target graph URIs. 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	protected void load(final MisteryGuest data) throws Exception {
		if (data.datasets == null || data.datasets.length == 0) {
			return;
		}
		
		for (final String datafileName : data.datasets) {
			if (data.graphURI != null) {
				localConnection.add(source(datafileName), DUMMY_BASE_URI, RDFFormat.TURTLE, localConnection.getValueFactory().createURI(data.graphURI));
				cumulusConnection.add(source(datafileName), DUMMY_BASE_URI, RDFFormat.TURTLE, cumulusConnection.getValueFactory().createURI(data.graphURI));
			} else {
				localConnection.add(source(datafileName), DUMMY_BASE_URI, RDFFormat.TURTLE);
				cumulusConnection.add(source(datafileName), DUMMY_BASE_URI, RDFFormat.TURTLE);				
			}
		}  
	} 
	
	protected abstract String examplesDirectory();
}