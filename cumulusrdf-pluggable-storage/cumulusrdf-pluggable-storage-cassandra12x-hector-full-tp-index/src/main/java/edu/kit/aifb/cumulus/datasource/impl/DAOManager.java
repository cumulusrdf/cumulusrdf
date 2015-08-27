package edu.kit.aifb.cumulus.datasource.impl;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * Counts the amount of open DAOs and closes the Hector cluster if all are
 * closed. This class is a singleton.
 * 
 * @author Sebastian Schmidt
 * @since 1.1.0
 */
public final class DAOManager {
	private static final DAOManager INSTANCE = new DAOManager();

	/**
	 * Returns the instance of the {@link DAOManager}.
	 * 
	 * @return the instance.
	 */
	public static DAOManager getInstance() {
		return INSTANCE;
	}

	private int _daoCount = 0;

	/**
	 * Singleton.
	 */
	private DAOManager() {
	}

	/**
	 * Informs the {@link DAOManager} that a DAO was created.
	 */
	public synchronized void daoWasCreated() {
		_daoCount++;
	}

	/**
	 * Informs the {@link DAOManager} that a DAO was closed. Shuts down the given cluster if all DAOs are closed.
	 * 
	 * @param cluster the cluster to shutdown if necessary.
	 */
	public synchronized void daoWasClosed(final Cluster cluster) {
		_daoCount--;

		if (_daoCount == 0) {
			HFactory.shutdownCluster(cluster);
		}
	}
}
