package edu.kit.aifb.cumulus.framework.mx;

import javax.management.MXBean;

/**
 * Management inteface of a CumulusRDF Store.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
@MXBean
public interface ManageableStore extends Manageable {
	/**
	 * Sets the maximum amount of triples or quads that should be inserted or deleted at once. 
	 * If an addition or deletion contains more triples or quads, the operation is splitted.
	 * 
	 * @param numOfTriplesPerBatch
	 *            The maximum number of triples or quads that should be handled
	 *            at once during data modifications.
	 */
	void setDefaultBatchLimit(final int numOfTriplesPerBatch);

	/**
	 * Returns the default maximum amount of triples or quads that should be inserted or deleted at once.
	 * 
	 * @return the default maximum amount of triples or quads that should be inserted or deleted at once.
	 */
	int getDefaultBatchLimit();

	/**
	 * Returns the amount of triples or quads in this store.
	 * 
	 * @return The amount of triples or quads in this store.
	 */
	long triplesCount();

	/**
	 * Returns true if this range indexes have been enabled.
	 * 
	 * @return true if this range indexes have been enabled.
	 */
	boolean isRangeIndexesSupportEnabled();

	/**
	 * Returns true if the store is connected to a Cassandra cluster.
	 * 
	 * @return true if the store is connected to a Cassandra cluster, false
	 *         otherwise.
	 */
	boolean isOpen();
	
	/**
	 * Returns true if there is currently an insertion or deletion happening,
	 * false otherwise.
	 * 
	 * @return true if there is currently an insertion or deletion happening,
	 *         false otherwise.
	 */
	boolean activeChanges();

	/**
	 * Returns the load worker throughput (in terms of triples / second).
	 * 
	 * @return the load worker throughput (in terms of triples / second).
	 */
	double getLoadWorkerThroughput();
	
	/**
	 * Returns the overall throughput of the latest bulk load.
	 * Overall means triples per second in terms of elapsed time between
	 * the start and the end of a given bulk load.
	 * 
	 * @return the overall throughput of the latest bulk load.
	 */
	double getLoadThroughput();	
}