package edu.kit.aifb.cumulus.webapp.endpoint.repository.namespaces;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import info.aduna.io.IOUtil;
import info.aduna.webapp.views.EmptySuccessView;
import info.aduna.webapp.views.SimpleResponseView;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.http.server.ClientHTTPException;
import org.openrdf.http.server.ServerHTTPException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
//import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.ModelAndView;

import edu.kit.aifb.cumulus.webapp.endpoint.SesameHTTPProtocolHandler;

/**
 * Handles requests for manipulating a specific namespace definition in a
 * repository.
 * 
 * The code is modified based on Sesame's org.openrdf.http.server.repository.namespaces.NamespaceController
 * 
 * @author Yongtao
 *
 */
public class NamespaceHandler extends SesameHTTPProtocolHandler {
	private Logger _logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public ModelAndView serve(final Repository repository, final HttpServletRequest request, final HttpServletResponse response)
			throws Exception {
		String pathInfoStr = request.getPathInfo();
		String[] pathInfo = pathInfoStr.substring(1).split("/");
		String prefix = pathInfo[pathInfo.length - 1];

		String reqMethod = request.getMethod();

		if (METHOD_HEAD.equals(reqMethod)) {
			_logger.info("HEAD namespace for prefix {}", prefix);

			Map<String, Object> model = new HashMap<String, Object>();
			return new ModelAndView(SimpleResponseView.getInstance(), model);
		}

		if (METHOD_GET.equals(reqMethod)) {
			_logger.info("GET namespace for prefix {}", prefix);
			return getExportNamespaceResult(repository, request, prefix);
		} else if ("PUT".equals(reqMethod)) {
			_logger.info("PUT prefix {}", prefix);
			return getUpdateNamespaceResult(repository, request, prefix);
		} else if ("DELETE".equals(reqMethod)) {
			_logger.info("DELETE prefix {}", prefix);
			return getRemoveNamespaceResult(repository, request, prefix);
		} else {
			throw new ServerHTTPException("Unexpected request method: " + reqMethod);
		}
	}

	/**
	 * 
	 * @param repository the Repository object
	 * @param request the HttpServletRequest object
	 * @param prefix the prefix
	 * @return the SimpleResponseView which will return the namesapce
	 * @throws ServerHTTPException throws when there is error about the repository
	 * @throws ClientHTTPException throws when there is undefined prefix
	 */
	private ModelAndView getExportNamespaceResult(final Repository repository, final HttpServletRequest request, final String prefix)
			throws ServerHTTPException, ClientHTTPException {
		try {
			String namespace = null;

			RepositoryConnection repositoryCon = repository.getConnection();
			synchronized (repositoryCon) {
				namespace = repositoryCon.getNamespace(prefix);
			}

			if (namespace == null) {
				throw new ClientHTTPException(SC_NOT_FOUND, "Undefined prefix: " + prefix);
			}

			Map<String, Object> model = new HashMap<String, Object>();
			model.put(SimpleResponseView.CONTENT_KEY, namespace);
			repositoryCon.close();
			return new ModelAndView(SimpleResponseView.getInstance(), model);
		} catch (RepositoryException e) {
			throw new ServerHTTPException("Repository error: " + e.getMessage(), e);
		}
	}

	/**
	 * update the namespace.
	 * 
	 * @param repository the Repository object
	 * @param request the HttpServletRequest object
	 * @param prefix the prefix
	 * @return EmptySuccessView object if success
	 * @throws IOException throws if there is error reading the namespace from the HttpServletRequest
	 * @throws ClientHTTPException throws if No namespace name found in request body
	 * @throws ServerHTTPException throws when there is error about the repository
	 */
	private ModelAndView getUpdateNamespaceResult(final Repository repository, final HttpServletRequest request, final String prefix)
			throws IOException, ClientHTTPException, ServerHTTPException {
		String namespace = IOUtil.readString(request.getReader());
		namespace = namespace.trim();

		if (namespace.length() == 0) {
			throw new ClientHTTPException(SC_BAD_REQUEST, "No namespace name found in request body");
		}

		try {
			RepositoryConnection repositoryCon = repository.getConnection();
			synchronized (repositoryCon) {
				repositoryCon.setNamespace(prefix, namespace);
			}
			repositoryCon.close();
		} catch (RepositoryException e) {
			throw new ServerHTTPException("Repository error: " + e.getMessage(), e);
		}

		return new ModelAndView(EmptySuccessView.getInstance());
	}

	/**
	 * remove the namespace.
	 * 
	 *@param repository the Repository object
	 * @param request the HttpServletRequest object
	 * @param prefix the prefix
	 * @return EmptySuccessView object if successvletRequest
	 * @throws ServerHTTPException throws when there is error about the repository
	 */
	private ModelAndView getRemoveNamespaceResult(final Repository repository, final HttpServletRequest request, final String prefix)
			throws ServerHTTPException {
		try {
			RepositoryConnection repositoryCon = repository.getConnection();
			synchronized (repositoryCon) {
				repositoryCon.removeNamespace(prefix);
			}
			repositoryCon.close();
		} catch (RepositoryException e) {
			throw new ServerHTTPException("Repository error: " + e.getMessage(), e);
		}

		return new ModelAndView(EmptySuccessView.getInstance());
	}

}
