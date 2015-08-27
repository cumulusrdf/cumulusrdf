package edu.kit.aifb.cumulus.store;

import java.util.HashMap;
import java.util.Map;

import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * A manager that manages multiple opened repositories.
 * 
 * @author Yongtao Ma
 * @since 1.1
 *
 */
public class RepositoryManager {
	private final Log _log = new Log(LoggerFactory.getLogger(RepositoryManager.class));

	private static RepositoryManager manager;
	/**the map that maintains multi repository**/
	private Map<String, Repository> _repoMap = new HashMap<String, Repository>();

	@SuppressWarnings("unused")
	private Repository _defaultRepository;

	/**
	 * Disable the public constructor.
	 */
	protected RepositoryManager() {
	}

	/**
	 * Gets the instance of RepositoryManager.
	 * @return the instance of RepositoryManager
	 */
	public static RepositoryManager getInstance() {
		if (manager == null) {
			manager = new RepositoryManager();
		}
		return manager;
	}

	/**
	 * Gets the repository according to the id.
	 * @param id the id of the repository
	 * @return the repository, null if there is no repository attached to the given id.
	 */
	public Repository getRepository(final String id) {
		return _repoMap.get(id);
	}

	/**
	 * Adds a repository to the manager.
	 * @param id the id of the repository
	 * @param repository the repository
	 * @param isDefault true if the repository is used as the default
	 */
	public void addRepository(final String id, final Repository repository, final boolean isDefault) {
		_repoMap.put(id, repository);
		if (isDefault) {
			_defaultRepository = repository;
		}
	}

	/**
	 * Closes all the repositories.
	 */
	public void shutDownAll() {
		synchronized (_repoMap) {
			for (String id : _repoMap.keySet()) {
				Repository repo = _repoMap.remove(id);
				if (repo != null) {
					shutdown(repo);
				}
			}
			_repoMap.clear();
		}
	}

	/**
	 * Shuts down a repository.
	 * Tries to shutdown the underlying store too.
	 * 
	 * @param repository the repository.
	 */
	void shutdown(final Repository repository) {

		if (repository != null) {
			_log.info(MessageCatalog._00012_REPOSITORY_SHUTDOWN_START);

			try {
				repository.shutDown();
				_log.info(MessageCatalog._00014_REPOSITORY_SHUTDOWN);
			} catch (final RepositoryException repositoryException) {
				_log.error(MessageCatalog._00013_REPOSITORY_SHUTDOWN_FAILURE, repositoryException);
			}
		}
	}
}
