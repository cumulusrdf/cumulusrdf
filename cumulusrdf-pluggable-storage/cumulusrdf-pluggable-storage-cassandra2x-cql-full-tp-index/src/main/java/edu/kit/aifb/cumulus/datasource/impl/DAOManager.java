package edu.kit.aifb.cumulus.datasource.impl;

/**
 * Counts the amount of open DAOs and closes the {@link com.datastax.driver.core.Cluster Cluster} if all are closed.
 * 
 * @author Sebastian Schmidt
 * @since 1.1.0
 */
public interface DAOManager {
	/**
	 * Informs the {@link DAOManager} that a DAO was closed.
	 */
	void daoWasClosed();
}
