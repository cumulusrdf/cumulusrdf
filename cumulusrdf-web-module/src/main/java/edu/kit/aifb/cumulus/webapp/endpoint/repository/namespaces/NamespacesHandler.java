package edu.kit.aifb.cumulus.webapp.endpoint.repository.namespaces;

import info.aduna.iteration.CloseableIteration;
import info.aduna.webapp.views.EmptySuccessView;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.http.server.ClientHTTPException;
import org.openrdf.http.server.ProtocolUtil;
import org.openrdf.http.server.ServerHTTPException;
import org.openrdf.http.server.repository.QueryResultView;
import org.openrdf.http.server.repository.TupleQueryResultView;
import org.openrdf.model.Literal;
import org.openrdf.model.Namespace;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.ListBindingSet;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.query.resultio.TupleQueryResultWriterFactory;
import org.openrdf.query.resultio.TupleQueryResultWriterRegistry;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
//import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.ModelAndView;

import edu.kit.aifb.cumulus.webapp.endpoint.SesameHTTPProtocolHandler;


/**
 * Handles requests for the list of namespace definitions for a repository.
 * 
 * The code is modified based on Sesame's org.openrdf.http.server.repository.namespaces.NamespacesController
 * 
 * @author Yongtao
 *
 */
public class NamespacesHandler extends SesameHTTPProtocolHandler{
	private Logger _logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public ModelAndView serve(final Repository repository, final HttpServletRequest request, final HttpServletResponse response)
			throws Exception {
		String reqMethod = request.getMethod();
		if (METHOD_GET.equals(reqMethod)) {
			_logger.info("GET namespace list");
			return getExportNamespacesResult(repository, request, response);
		}
		if (METHOD_HEAD.equals(reqMethod)) {
			_logger.info("HEAD namespace list");
			return getExportNamespacesResult(repository, request, response);
		} else if ("DELETE".equals(reqMethod)) {
			_logger.info("DELETE namespaces");
			return getClearNamespacesResult(repository, request, response);
		}

		throw new ClientHTTPException(HttpServletResponse.SC_METHOD_NOT_ALLOWED, "Method not allowed: "
				+ reqMethod);
	}

	/**
	 * 
	 * @param repository the Repository object
	 * @param request the HttpServletRequest object 
	 * @param response the HttpServletResponse oject
	 * @return QueryResultView object include the namespaces
	 * @throws ClientHTTPException throws when errors in the request
	 * @throws ServerHTTPException throws when errors in the Repository
	 * @throws RepositoryException throws when errors in closing the RepositoryConnection 
	 */
	private ModelAndView getExportNamespacesResult(final Repository repository, final HttpServletRequest request, final HttpServletResponse response)
			throws ClientHTTPException, ServerHTTPException, RepositoryException {
		final boolean headersOnly = METHOD_HEAD.equals(request.getMethod());

		Map<String, Object> model = new HashMap<String, Object>();
		if (!headersOnly) {
			List<String> columnNames = Arrays.asList("prefix", "namespace");
			List<BindingSet> namespaces = new ArrayList<BindingSet>();

			RepositoryConnection repositoryCon = repository.getConnection();
			synchronized (repositoryCon) {
				try {
					CloseableIteration<? extends Namespace, RepositoryException> iter = repositoryCon.getNamespaces();
	
					try {
						while (iter.hasNext()) {
							Namespace ns = iter.next();
	
							Literal prefix = new LiteralImpl(ns.getPrefix());
							Literal namespace = new LiteralImpl(ns.getName());
	
							BindingSet bindingSet = new ListBindingSet(columnNames, prefix, namespace);
							namespaces.add(bindingSet);
						}
					} finally {
						iter.close();
					}
				} catch (RepositoryException e) {
					throw new ServerHTTPException("Repository error: " + e.getMessage(), e);
				}
			}
			model.put(QueryResultView.QUERY_RESULT_KEY, new TupleQueryResultImpl(columnNames, namespaces));
			repositoryCon.close();
		}

		TupleQueryResultWriterFactory factory = ProtocolUtil.getAcceptableService(request, response,
				TupleQueryResultWriterRegistry.getInstance());

		model.put(QueryResultView.FILENAME_HINT_KEY, "namespaces");
		model.put(QueryResultView.HEADERS_ONLY, headersOnly);
		model.put(QueryResultView.FACTORY_KEY, factory);

		return new ModelAndView(TupleQueryResultView.getInstance(), model);
	}

	/**
	 * clear the namespaces
	 * 
	 * @param repository the Repository object
	 * @param request the HttpServletRequest object 
	 * @param response the HttpServletResponse oject
	 * @return EmptySuccessView object if success
	 * @throws ServerHTTPException throws when errors in repository
	 * @throws RepositoryException throws when errors in closing the RepositoryConnection 
	 */
	private ModelAndView getClearNamespacesResult(final Repository repository, final HttpServletRequest request, final HttpServletResponse response)
			throws ServerHTTPException, RepositoryException {
		RepositoryConnection repositoryCon = repository.getConnection();
		synchronized (repositoryCon) {
			try {
				repositoryCon.clearNamespaces();
			} catch (RepositoryException e) {
				throw new ServerHTTPException("Repository error: " + e.getMessage(), e);
			}
		}
		repositoryCon.close();
		return new ModelAndView(EmptySuccessView.getInstance());
	}

}
