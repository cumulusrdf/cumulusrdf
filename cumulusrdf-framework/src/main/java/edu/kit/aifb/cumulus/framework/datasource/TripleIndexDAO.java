package edu.kit.aifb.cumulus.framework.datasource;

import java.util.Iterator;
import java.util.List;

import org.openrdf.model.Value;

/**
 * Triple Index data access object interface.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public interface TripleIndexDAO {

	/**
	 * Initialise the underlying RDF index.
	 * That could mean different things depending on the concrete kind of underlying storage. 
	 * However, the goal of this method is to give a chanche (once) to the system, to check if
	 * all required persistent entities (e.g. tables, column families, schema) are already there and eventually create them.
	 * 
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void initialiseRdfIndex() throws DataAccessLayerException;
	
	/**
	 * Inserts in the index the given statement.
	 * Inserts in the index the given triple, where each RDF value is encoded as a byte[] by the dictionary. 
	 * That is, each triple[0] = subject, triple[1] = predicate, and triple[2] = object.
	 * 
	 * @param ids the triple identifiers.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void insertTriple(byte[][] ids) throws DataAccessLayerException;

	/**
	 * Inserts in the index the given range statements (as identifiers).
	 * 
	 * @param ids the triple identifiers.
	 * @param value the range value.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void insertRanges(byte[][] ids, double value) throws DataAccessLayerException;
	
	/**
	 * Inserts in the index the given range statements (as identifiers).
	 * 
	 * @param ids the triple identifiers.
	 * @param value the range value.
	 * @throws DataAccessLayerException in case of data access failure.
	 */	
	void insertRanges(byte[][] ids, long value) throws DataAccessLayerException;
	
	/**
	 * Deletes from the index the given nodes (given as identifiers).
	 * 
	 * @param nodes the iterator containing the triple identifiers.
	 * @param batchSize the batch size we will use in deletion.
	 * @param rangesEnabled if ranges have been enabled on this CumulusRDF instance.
	 * @return the list of triples (as identifiers) that have been deleted.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	List<byte[][]> deleteTriples(final Iterator<byte[][]> nodes, int batchSize, boolean rangesEnabled) throws DataAccessLayerException;

	/**
	 * Executes a batch of previously accumulated mutations.
	 * Some data access drivers (specific to some storages / clients) works using an accumulator pattern.
	 * CumulusRDF core will call this method in order to make sure also that kind of client will have a chance to
	 * execute accumulated mutation.
	 * Clients that do not act like that, can simply leave empty this method.  
	 * 
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void executePendingMutations() throws DataAccessLayerException;
	
	/**
	 * Executes a query.
	 * 
	 * @param query the query pattern.
	 * @param limit the max number of triples in result.
	 * @return an iterator containing query results.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	Iterator<byte[][]> query(byte[][] query, int limit) throws DataAccessLayerException;
	
	/**
	 * Executes a numeric range query.
	 * 
	 * @param query the query pattern.
	 * @param lowerBound the lower bound of the requested range.
	 * @param equalsLower if lower bound should be included.
	 * @param upperBound the higher bound of the requested range.
	 * @param equalsUpper if higher bound should be included.
	 * @param reverse the order by criteria.
	 * @param limit the max number of triples in result.
	 * @return an iterator containing query results (as identifiers).
	 * @throws DataAccessLayerException in case of data access failure.
	 */	
	Iterator<byte[][]> numericRangeQuery(
			Value[] query, 
			double lowerBound, boolean equalsLower, 
			double upperBound, boolean equalsUpper,
			boolean reverse, 
			int limit) throws DataAccessLayerException;
	
	/**
	 * Executes a date range query.
	 * 
	 * @param query the query pattern.
	 * @param lowerBound the lower bound of the requested range.
	 * @param equalsLower if lower bound should be included.
	 * @param upperBound the higher bound of the requested range.
	 * @param equalsUpper if higher bound should be included.
	 * @param reverse the order by criteria.
	 * @param limit the max number of triples in result.
	 * @return an iterator containing query results (as identifiers).
	 * @throws DataAccessLayerException in case of data access failure.
	 */	
	Iterator<byte[][]> dateRangeQuery(
			Value[] query, long lowerBound,
			boolean equalsLower, long upperBound, boolean equalsUpper,
			boolean reverse, int limit) throws DataAccessLayerException;	
	
	/**
	 * Gives a chance to this DAO to close and release acquired resources.
	 * 
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void close() throws DataAccessLayerException;

	/**
	 * Clears all data managed from the underlying index.
	 */
	void clear();	
}