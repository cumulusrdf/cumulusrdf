package edu.kit.aifb.cumulus.webapp.endpoint.repository;

import info.aduna.lang.FileFormat;
import info.aduna.lang.service.FileFormatServiceRegistry;
import info.aduna.webapp.util.HttpServerUtil;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.http.protocol.Protocol;
import org.openrdf.http.protocol.error.ErrorInfo;
import org.openrdf.http.protocol.error.ErrorType;
import org.openrdf.http.server.ClientHTTPException;
import org.openrdf.http.server.HTTPException;
import org.openrdf.http.server.ProtocolUtil;
import org.openrdf.http.server.ServerHTTPException;
import org.openrdf.http.server.repository.BooleanQueryResultView;
import org.openrdf.http.server.repository.GraphQueryResultView;
import org.openrdf.http.server.repository.QueryResultView;
import org.openrdf.http.server.repository.TupleQueryResultView;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryInterruptedException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.UnsupportedQueryLanguageException;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.resultio.BooleanQueryResultWriterRegistry;
import org.openrdf.query.resultio.TupleQueryResultWriterRegistry;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.manager.RepositoryManager;
import org.openrdf.rio.RDFWriterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.View;

import edu.kit.aifb.cumulus.webapp.endpoint.SesameHTTPProtocolHandler;

/**
 * Handles queries and admin (delete) operations on a repository and renders the
 * results in a format suitable to the type of operation.
 * 
 * The code is modified based on Sesame's org.openrdf.http.server.repository.RepositoryController
 * 
 * @author Yongtao
 *
 */
public class RepositoryHandler extends SesameHTTPProtocolHandler {
	private Logger _logger = LoggerFactory.getLogger(this.getClass());

	private RepositoryManager _repositoryManager;

	private static final String METHOD_DELETE = "DELETE";

	/**
	 * set the Repository Manager.
	 * 
	 * currenttly this function is not called since cumulusRDF server only support one Repository at the moment
	 * @param repMan the RepositoryManager object
	 */
	public void setRepositoryManager(final RepositoryManager repMan) {
		_repositoryManager = repMan;
	}

	@Override
	public ModelAndView serve(final Repository repository, final HttpServletRequest request, final HttpServletResponse response)
			throws Exception {
		String reqMethod = request.getMethod();
		String queryStr = request.getParameter(QUERY_PARAM_NAME);

		if (METHOD_DELETE.equals(reqMethod)) {
			throw new ServerHTTPException("Unsupportted delete repository operation");
		}

		int qryCode = 0;
		if (_logger.isInfoEnabled() || _logger.isDebugEnabled()) {
			qryCode = String.valueOf(queryStr).hashCode();
		}

		boolean headersOnly = false;
		if (METHOD_GET.equals(reqMethod)) {
			_logger.info("GET query {}", qryCode);
		} else if (METHOD_HEAD.equals(reqMethod)) {
			_logger.info("HEAD query {}", qryCode);
			headersOnly = true;
		} else if (METHOD_POST.equals(reqMethod)) {
			_logger.info("POST query {}", qryCode);

			String mimeType = HttpServerUtil.getMIMEType(request.getContentType());
			if (!Protocol.FORM_MIME_TYPE.equals(mimeType)) {
				throw new ClientHTTPException(response.SC_UNSUPPORTED_MEDIA_TYPE, "Unsupported MIME type: " + mimeType);
			}
		}

		_logger.debug("query {} = {}", qryCode, queryStr);

		if (queryStr != null) {
			RepositoryConnection repositoryCon = repository.getConnection();
			synchronized (repositoryCon) {
				Query query = getQuery(repository, repositoryCon, queryStr, request, response);

				View view;
				Object queryResult;
				FileFormatServiceRegistry<? extends FileFormat, ?> registry;

				try {
					if (query instanceof TupleQuery) {
						TupleQuery tQuery = (TupleQuery) query;

						queryResult = headersOnly ? null : tQuery.evaluate();
						registry = TupleQueryResultWriterRegistry.getInstance();
						view = TupleQueryResultView.getInstance();
					} else if (query instanceof GraphQuery) {
						GraphQuery gQuery = (GraphQuery) query;

						queryResult = headersOnly ? null : gQuery.evaluate();
						registry = RDFWriterRegistry.getInstance();
						view = GraphQueryResultView.getInstance();
					} else if (query instanceof BooleanQuery) {
						BooleanQuery bQuery = (BooleanQuery) query;

						queryResult = headersOnly ? null : bQuery.evaluate();
						registry = BooleanQueryResultWriterRegistry.getInstance();
						view = BooleanQueryResultView.getInstance();
					} else {
						throw new ClientHTTPException(response.SC_BAD_REQUEST, "Unsupported query type: "
								+ query.getClass().getName());
					}
				} catch (QueryInterruptedException e) {
					_logger.info("Query interrupted", e);
					throw new ServerHTTPException(response.SC_SERVICE_UNAVAILABLE, "Query evaluation took too long");
				} catch (QueryEvaluationException e) {
					_logger.info("Query evaluation error", e);
					if (e.getCause() != null && e.getCause() instanceof HTTPException) {
						// custom signal from the backend, throw as HTTPException
						// directly (see SES-1016).
						throw (HTTPException) e.getCause();
					} else {
						throw new ServerHTTPException("Query evaluation error: " + e.getMessage());
					}
				}
				Object factory = ProtocolUtil.getAcceptableService(request, response, registry);

				Map<String, Object> model = new HashMap<String, Object>();
				model.put(QueryResultView.FILENAME_HINT_KEY, "query-result");
				model.put(QueryResultView.QUERY_RESULT_KEY, queryResult);
				model.put(QueryResultView.FACTORY_KEY, factory);
				model.put(QueryResultView.HEADERS_ONLY, headersOnly);
				return new ModelAndView(view, model);
			}
		} else {
			throw new ClientHTTPException(response.SC_BAD_REQUEST, "Missing parameter: " + QUERY_PARAM_NAME);
		}
	}

	/**
	 * prepare a query according to the queryStr
	 * 
	 * @param repository the Repository object
	 * @param repositoryCon the RepositoryConnection object
	 * @param queryStr the String value of the query
	 * @param request the HttpServletRequest object
	 * @param response the HttpServletResponse object
	 * @return the parsed query
	 * @throws IOException throws when there are repository errors
	 * @throws ClientHTTPException throws when errors in parsing the queryStr
	 */
	private Query getQuery(final Repository repository, final RepositoryConnection repositoryCon, final String queryStr,
			final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, ClientHTTPException {
		Query result = null;

		// default query language is SPARQL
		QueryLanguage queryLn = QueryLanguage.SPARQL;

		String queryLnStr = request.getParameter(QUERY_LANGUAGE_PARAM_NAME);
		_logger.debug("query language param = {}", queryLnStr);

		if (queryLnStr != null) {
			queryLn = QueryLanguage.valueOf(queryLnStr);

			if (queryLn == null) {
				throw new ClientHTTPException(response.SC_BAD_REQUEST, "Unknown query language: " + queryLnStr);
			}
		}

		String baseURI = request.getParameter(Protocol.BASEURI_PARAM_NAME);

		// determine if inferred triples should be included in query evaluation
		boolean includeInferred = ProtocolUtil.parseBooleanParam(request, INCLUDE_INFERRED_PARAM_NAME, true);

		String timeout = request.getParameter(Protocol.TIMEOUT_PARAM_NAME);
		int maxQueryTime = 0;
		if (timeout != null) {
			try {
				maxQueryTime = Integer.parseInt(timeout);
			} catch (NumberFormatException e) {
				throw new ClientHTTPException(response.SC_BAD_REQUEST, "Invalid timeout value: " + timeout);
			}
		}

		// build a dataset, if specified
		String[] defaultGraphURIs = request.getParameterValues(DEFAULT_GRAPH_PARAM_NAME);
		String[] namedGraphURIs = request.getParameterValues(NAMED_GRAPH_PARAM_NAME);

		DatasetImpl dataset = null;
		if (defaultGraphURIs != null || namedGraphURIs != null) {
			dataset = new DatasetImpl();

			if (defaultGraphURIs != null) {
				for (String defaultGraphURI : defaultGraphURIs) {
					try {
						URI uri = createURIOrNull(repository, defaultGraphURI);
						dataset.addDefaultGraph(uri);
					} catch (IllegalArgumentException e) {
						throw new ClientHTTPException(response.SC_BAD_REQUEST, "Illegal URI for default graph: "
								+ defaultGraphURI);
					}
				}
			}

			if (namedGraphURIs != null) {
				for (String namedGraphURI : namedGraphURIs) {
					try {
						URI uri = createURIOrNull(repository, namedGraphURI);
						dataset.addNamedGraph(uri);
					} catch (IllegalArgumentException e) {
						throw new ClientHTTPException(response.SC_BAD_REQUEST, "Illegal URI for named graph: "
								+ namedGraphURI);
					}
				}
			}
		}

		try {
			result = repositoryCon.prepareQuery(queryLn, queryStr, baseURI);

			result.setIncludeInferred(includeInferred);

			if (maxQueryTime > 0) {
				result.setMaxQueryTime(maxQueryTime);
			}

			if (dataset != null) {
				result.setDataset(dataset);
			}

			// determine if any variable bindings have been set on this query.
			@SuppressWarnings("unchecked")
			Enumeration<String> parameterNames = request.getParameterNames();

			while (parameterNames.hasMoreElements()) {
				String parameterName = parameterNames.nextElement();

				if (parameterName.startsWith(BINDING_PREFIX) && parameterName.length() > BINDING_PREFIX.length())
				{
					String bindingName = parameterName.substring(BINDING_PREFIX.length());
					Value bindingValue = ProtocolUtil.parseValueParam(request, parameterName,
							repository.getValueFactory());
					result.setBinding(bindingName, bindingValue);
				}
			}
		} catch (UnsupportedQueryLanguageException e) {
			ErrorInfo errInfo = new ErrorInfo(ErrorType.UNSUPPORTED_QUERY_LANGUAGE, queryLn.getName());
			throw new ClientHTTPException(response.SC_BAD_REQUEST, errInfo.toString());
		} catch (MalformedQueryException e) {
			ErrorInfo errInfo = new ErrorInfo(ErrorType.MALFORMED_QUERY, e.getMessage());
			throw new ClientHTTPException(response.SC_BAD_REQUEST, errInfo.toString());
		} catch (RepositoryException e) {
			_logger.error("Repository error", e);
			response.sendError(response.SC_INTERNAL_SERVER_ERROR);
		}

		return result;
	}

	/**
	 * @param repository the Repository object
	 * @param graphURI the String the graphURI to be created
	 * @return the created URI for the input graphURI
	 */
	private URI createURIOrNull(final Repository repository, final String graphURI) {
		if ("null".equals(graphURI)) {
			return null;
		}
		return repository.getValueFactory().createURI(graphURI);
	}

}
