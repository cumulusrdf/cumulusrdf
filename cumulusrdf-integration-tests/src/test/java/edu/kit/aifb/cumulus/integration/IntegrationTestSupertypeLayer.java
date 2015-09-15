/**
 * This is part of the Integration Test suite developed as part of the SolRDF [1] project.
 * 
 * [1] https://github.com/agazzarini/SolRDF
 */
package edu.kit.aifb.cumulus.integration;

import static edu.kit.aifb.cumulus.test.TestUtility.DUMMY_BASE_URI;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import info.aduna.iteration.Iterations;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.util.ModelUtil;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResults;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
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
		try {
			assertTrue(
					ModelUtil.equals(
							statements(Iterations.asSet(localResult)), 
							statements(Iterations.asSet(cumulusResult))));			
		} catch (final AssertionError exception) {
			final GraphQueryResult debugLocalResult = localQuery.evaluate();
			final GraphQueryResult debugCumulusResult = cumulusQuery.evaluate();
				
			System.err.println("***** LOCAL ******");
			QueryResultIO.write(debugLocalResult, RDFFormat.NTRIPLES, System.err);

			System.err.println("***** CRDF ******");
			QueryResultIO.write(debugCumulusResult, RDFFormat.NTRIPLES, System.err);
			
			debugCumulusResult.close();
			debugLocalResult.close();
			throw exception;
		}
		
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
		
		final TupleQueryResult localResult = localQuery.evaluate();
		final TupleQueryResult cumulusResult = cumulusQuery.evaluate();
		
		try {
			assertTrue(QueryResults.equals(localResult, cumulusResult));
		} catch (final AssertionError exception) {
			final TupleQueryResult debugLocalResult = localQuery.evaluate();
			final TupleQueryResult debugCumulusResult = cumulusQuery.evaluate();
				
			System.err.println("***** LOCAL ******");
			QueryResultIO.write(debugLocalResult, TupleQueryResultFormat.JSON, System.err);

			System.err.println("***** CRDF ******");
			QueryResultIO.write(debugCumulusResult, TupleQueryResultFormat.JSON, System.err);
			
			debugCumulusResult.close();
			debugLocalResult.close();
			throw exception;
		}
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
			final RDFFormat format = datafileName.endsWith(".ttl") ? RDFFormat.TURTLE : RDFFormat.RDFXML;
			if (data.graphURI != null) {
				localConnection.add(source(datafileName), DUMMY_BASE_URI, format, localConnection.getValueFactory().createURI(data.graphURI));
				cumulusConnection.add(source(datafileName), DUMMY_BASE_URI, format, cumulusConnection.getValueFactory().createURI(data.graphURI));
			} else {
				localConnection.add(source(datafileName), DUMMY_BASE_URI, format);
				cumulusConnection.add(source(datafileName), DUMMY_BASE_URI, format);				
			}
		}  
	} 
	
	/**
	 * Returns the directory where this tests will look for sample datafiles.
	 * 
	 * @return the directory where this tests will look for sample datafiles.
	 */
	protected abstract String examplesDirectory();
	
	/**
	 * Creates an in-memory clone of the incoming collection of statements. 
	 * This is needed in order to execute comparisons between statements belonging to different value factories (e.g. InMemory and CumulusRDF).
	 * 
	 * @param model the set of statements to be cloned.
	 * @return an in-memory clone of the incoming collection of statements.
	 */
	Set<Statement> statements(final Set<? extends Statement> model) {
		final ValueFactory factory = inMemoryRepository.getValueFactory();
		return model
				.stream()
				.map(statement -> {
						Value object = null;
						if (statement.getObject() instanceof Literal) {
							Literal l = (Literal) statement.getObject();
							if (l.getLanguage() != null) {
								object = factory.createLiteral(l.getLabel(), l.getLanguage());								
							} else {
								object = factory.createLiteral(l.getLabel(), l.getDatatype());																
							}
						} else if (statement.getObject() instanceof BNode) {
							object = factory.createBNode(((BNode)statement.getObject()).getID()); 
						} else if (statement.getObject() instanceof URI) {
							object = factory.createURI(((URI)statement.getSubject()).stringValue());
						}
						
						return factory.createStatement(
							statement.getSubject() instanceof BNode 
								? factory.createBNode(((BNode)statement.getSubject()).getID()) 
								: factory.createURI(((URI)statement.getSubject()).stringValue()), 
							factory.createURI(((URI)statement.getPredicate()).stringValue()), 
							object);
				})
				.collect(toSet());
	}
}