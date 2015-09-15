/*
 * This Test case makes use of some examples from 
 * 
 * "Learning SPARQL - Querying and Updating with SPARQL 1.1" by Bob DuCharme
 * 
 * Publisher: O'Reilly
 * Author: Bob DuCharme
 * ISBN: 978-1-449-37143-2
 * http://www.learningsparql.com/
 * http://shop.oreilly.com/product/0636920030829.do
 * 
 * We warmly appreciate and thank the author and O'Reilly for such permission.
 * 
 */
package edu.kit.aifb.cumulus.integration.learningSPARQL;

import static edu.kit.aifb.cumulus.test.MisteryGuest.misteryGuest;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.openrdf.model.util.ModelUtil;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.rio.RDFFormat;

import edu.kit.aifb.cumulus.test.MisteryGuest;

/**
 * SPARQL Update Integration tests using examples taken from LearningSPARQL book.
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 * @see http://learningsparql.com
 */  
public class LearningSparql_UPDATE_ITCase extends LearningSparqlSupertypeLayer {
	
	/**
	 * Executes a given update command both on remote and local model.
	 * 
	 * @param data the object holding test data (i.e. commands, queries, datafiles).
	 * @throws Exception hopefully never otherwise the corresponding test fails.
	 */
	@SuppressWarnings("deprecation")
	void executeUpdate(final MisteryGuest data) throws Exception {
		load(data);
		
		final String updateCommand = readFile(data.query);
		final Update localUpdate = localConnection.prepareUpdate(QueryLanguage.SPARQL, updateCommand);
		final Update cumulusUpdate = cumulusConnection.prepareUpdate(QueryLanguage.SPARQL, updateCommand);
		
		localUpdate.execute();
		cumulusUpdate.execute();
		
		try {
			assertTrue(ModelUtil.equals(
					statements(localConnection.getStatements(null, null, null, false).asList()), 
					statements(cumulusConnection.getStatements(null, null, null, false).asList())));
			
		} catch (final AssertionError exception) {
			final String queryString = "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o}";
			final GraphQuery localQuery = localConnection.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
			final GraphQuery cumulusQuery = cumulusConnection.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
			
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
	}
	
	@Test
	public void insertData() throws Exception {
		executeUpdate(misteryGuest("ex312.ru"));
	}
	
	@Test
	public void insert() throws Exception {
		executeUpdate(misteryGuest("ex313.ru", "ex012.ttl"));
	}	
	
	@Test
	public void insertAsConstructThatChangesData() throws Exception {
		executeUpdate(misteryGuest("ex316.ru", "ex012.ttl"));
	}
	
	@Test
	public void loadKeyword() throws Exception {
		executeUpdate(misteryGuest("ex546.ru"));
	}	
	
	@Test
	public void deleteData() throws Exception {
		selectTest(misteryGuest("ex547.rq", "exxyz.ttl"));
		executeUpdate(misteryGuest("ex548.ru"));
		selectTest(misteryGuest("ex547.rq"));		
	}	
	
	@Test
	public void delete_I() throws Exception {
		executeUpdate(misteryGuest("ex549.ru", "exxyz.ttl"));
	}		
	
	@Test
	public void delete_II() throws Exception {
		executeUpdate(misteryGuest("ex550.ru", "exxyz.ttl"));
	}			
	
	@Test
	public void delete_III() throws Exception {
		executeUpdate(misteryGuest("ex551.ru", "exxyz.ttl"));
	}

	@Test
	public void clear() throws Exception {
		executeUpdate(misteryGuest("ex324.ru", "ex012.ttl"));
	}
	
	@Test
	public void changingExistingData() throws Exception {
		selectTest(misteryGuest("ex311.rq", "ex012.ttl"));
		constructTest(misteryGuest("ex326.rq"));
		executeUpdate(misteryGuest("ex325.ru"));
		selectTest(misteryGuest("ex311.rq"));
	}
	
	@Test
	public void changingSkosInSkosXL() throws Exception {
		executeUpdate(misteryGuest("ex329.ru", "ex327.ttl"));
	}	
	
	@Test
	public void insertDataInNamedGraphs() throws Exception {
		executeUpdate(misteryGuest("ex330.ru"));
		executeUpdate(misteryGuest("ex331.ru"));
		executeUpdate(misteryGuest("ex333.ru"));
		selectTest(misteryGuest("ex332.rq"));
	}	

	@Test
	public void insertInNamedGraphs() throws Exception {
		executeUpdate(misteryGuest("ex330.ru"));
		executeUpdate(misteryGuest("ex543.ru"));
		selectTest(misteryGuest("ex332.rq"));
	}	

	@Test
	public void droppingNamedGraph() throws Exception {
		executeUpdate(misteryGuest("ex330.ru"));
		executeUpdate(misteryGuest("ex331.ru"));
		executeUpdate(misteryGuest("ex334.ru"));
		executeUpdate(misteryGuest("ex335.ru"));
		selectTest(misteryGuest("ex332.rq"));
	}	

	@Test
	public void droppingDefaultGraph() throws Exception {
		executeUpdate(misteryGuest("ex330.ru"));
		executeUpdate(misteryGuest("ex331.ru"));
		executeUpdate(misteryGuest("ex334.ru"));
		executeUpdate(misteryGuest("ex336.ru"));
		selectTest(misteryGuest("ex332.rq"));
	}	
	
	@Test
	public void droppingAllNamedGraphs() throws Exception {
		executeUpdate(misteryGuest("ex338.ru"));
		executeUpdate(misteryGuest("ex339.ru"));
		selectTest(misteryGuest("ex332.rq"));
	}	

	@Test
	public void createGraph() throws Exception {
		executeUpdate(misteryGuest("ex340.ru"));
		selectTest(misteryGuest("ex332.rq"));
	}	

	@Test
	public void withKeyword() throws Exception {
		executeUpdate(misteryGuest("ex342.ru"));
	}	
	
	@Test
	public void usingKeyword() throws Exception {
		executeUpdate(misteryGuest("ex342.ru"));
		executeUpdate(misteryGuest("ex343.ru"));
		selectTest(misteryGuest("ex332.rq"));
	}	
	
	@Test
	public void usingNamedKeyword() throws Exception {
		executeUpdate(misteryGuest("ex342.ru"));
		executeUpdate(misteryGuest("ex345.ru"));
		selectTest(misteryGuest("ex332.rq"));
	}	
	
	@Test
	public void copyingGraphs() throws Exception {
		executeUpdate(misteryGuest("ex338.ru"));
		executeUpdate(misteryGuest("ex503.ru"));		
	}	

	@Test
	public void movingGraphs() throws Exception {
		executeUpdate(misteryGuest("ex338.ru"));
		executeUpdate(misteryGuest("ex505.ru"));	
	}		
	
	@Test
	public void deletingGraphsWithDeleteData() throws Exception {
		executeUpdate(misteryGuest("ex338.ru"));
		executeUpdate(misteryGuest("ex346.ru"));	
		selectTest(misteryGuest("ex332.rq"));
	}	
	
	@Test
	public void deletingGraphsWithDelete() throws Exception {
		executeUpdate(misteryGuest("ex338.ru"));
		executeUpdate(misteryGuest("ex347.ru"));	
		selectTest(misteryGuest("ex332.rq"));
	}	
	
	@Test
	public void deletingGraphsWithDeleteAndWith() throws Exception {
		executeUpdate(misteryGuest("ex338.ru"));
		executeUpdate(misteryGuest("ex348.ru"));	
		selectTest(misteryGuest("ex332.rq"));
	}	
	
	@Test
	public void replaceData() throws Exception {
		executeUpdate(misteryGuest("ex338.ru"));
		executeUpdate(misteryGuest("ex349.ru"));	
		selectTest(misteryGuest("ex332.rq"));
	}	
	
	@Test
	public void replaceDataWith() throws Exception {
		executeUpdate(misteryGuest("ex338.ru"));
		executeUpdate(misteryGuest("ex350.ru"));	
		selectTest(misteryGuest("ex332.rq"));
	}	
	
	@Test
	public void dropAllAndCreateSomeData() throws Exception {
		executeUpdate(misteryGuest("ex351.ru"));
		selectTest(misteryGuest("ex332.rq"));
	}
	
	@Test
	public void dropAllInsertAndChangeSomeData_I() throws Exception {
		executeUpdate(misteryGuest("ex351.ru"));
		executeUpdate(misteryGuest("ex352.ru"));
		selectTest(misteryGuest("ex332.rq"));
	}	
	
	@Test
	public void dropAllInsertAndChangeSomeData_II() throws Exception {
		executeUpdate(misteryGuest("ex351.ru"));
		executeUpdate(misteryGuest("ex353.ru"));
		selectTest(misteryGuest("ex354.rq"));
	}		
}