package edu.kit.aifb.cumulus.webapp.endpoint.repository.statements;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE;
import info.aduna.webapp.util.HttpServerUtil;
import info.aduna.webapp.views.EmptySuccessView;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.http.protocol.Protocol;
import org.openrdf.http.protocol.error.ErrorInfo;
import org.openrdf.http.protocol.error.ErrorType;
import org.openrdf.http.protocol.transaction.TransactionReader;
import org.openrdf.http.protocol.transaction.operations.TransactionOperation;
import org.openrdf.http.server.ClientHTTPException;
import org.openrdf.http.server.HTTPException;
import org.openrdf.http.server.ProtocolUtil;
import org.openrdf.http.server.ServerHTTPException;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
//import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.rio.Rio;
import org.openrdf.rio.UnsupportedRDFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.ModelAndView;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import edu.kit.aifb.cumulus.webapp.endpoint.SesameHTTPProtocolHandler;

/**
 * Handles requests for manipulating the statements in a repository.
 * The class is modified based on Sesame's {@link org.openrdf.http.server.repository.statements.StatementsController}.
 * 
 * @author Yongtao Ma
 * @since 1.1
 * @see {@link org.openrdf.http.server.repository.statements.StatementsController}
 */
public class StatementHandler extends SesameHTTPProtocolHandler {

	private Logger _logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public ModelAndView serve(final Repository repository, final HttpServletRequest request, final HttpServletResponse response)
			throws Exception {
		ModelAndView result;

		String reqMethod = request.getMethod();

		if (METHOD_GET.equals(reqMethod)) {
			_logger.info("GET statements");
			result = getExportStatementsResult(repository, request, response);
		} else if (METHOD_HEAD.equals(reqMethod)) {
			_logger.info("HEAD statements");
			result = getExportStatementsResult(repository, request, response);
		} else if (METHOD_POST.equals(reqMethod)) {
			String mimeType = HttpServerUtil.getMIMEType(request.getContentType());

			if (Protocol.TXN_MIME_TYPE.equals(mimeType)) {
				_logger.info("POST transaction to repository");
				result = getTransactionResultResult(repository, request, response);
			} else if (request.getParameterMap().containsKey(Protocol.UPDATE_PARAM_NAME)) {
				_logger.info("POST SPARQL update request to repository");
				result = getSparqlUpdateResult(repository, request, response);
			} else {
				_logger.info("POST data to repository");
				result = getAddDataResult(repository, request, response, false);
			}
		} else if ("PUT".equals(reqMethod)) {
			_logger.info("PUT data in repository");
			result = getAddDataResult(repository, request, response, false);
		} else if ("DELETE".equals(reqMethod)) {
			_logger.info("DELETE data from repository");
			result = getDeleteDataResult(repository, request, response);
		} else {
			throw new ClientHTTPException(HttpServletResponse.SC_METHOD_NOT_ALLOWED, "Method not allowed: "
					+ reqMethod);
		}

		return result;
	}

	/**
	 * update the data according to Sqarql.
	 * 
	 * @param repository the Repository object
	 * @param request the HttpServletRequest object
	 * @param response the HttpServletResponse object
	 * @return the EmptySuccessView if success
	 * @throws HTTPException throws when there are illegal URI
	 */
	private ModelAndView getSparqlUpdateResult(final Repository repository, final HttpServletRequest request,
			final HttpServletResponse response)
			throws HTTPException {
		ProtocolUtil.logRequestParameters(request);

		String sparqlUpdateString = request.getParameterValues(Protocol.UPDATE_PARAM_NAME)[0];

		// default query language is SPARQL
		QueryLanguage queryLn = QueryLanguage.SPARQL;

		String queryLnStr = request.getParameter(QUERY_LANGUAGE_PARAM_NAME);
		_logger.debug("query language param = {}", queryLnStr);

		if (queryLnStr != null) {
			queryLn = QueryLanguage.valueOf(queryLnStr);

			if (queryLn == null) {
				throw new ClientHTTPException(SC_BAD_REQUEST, "Unknown query language: " + queryLnStr);
			}
		}

		String baseURI = request.getParameter(Protocol.BASEURI_PARAM_NAME);

		// determine if inferred triples should be included in query evaluation
		boolean includeInferred = ProtocolUtil.parseBooleanParam(request, INCLUDE_INFERRED_PARAM_NAME, true);

		// build a dataset, if specified
		String[] defaultRemoveGraphURIs = request.getParameterValues(REMOVE_GRAPH_PARAM_NAME);
		String[] defaultInsertGraphURIs = request.getParameterValues(INSERT_GRAPH_PARAM_NAME);
		String[] defaultGraphURIs = request.getParameterValues(USING_GRAPH_PARAM_NAME);
		String[] namedGraphURIs = request.getParameterValues(USING_NAMED_GRAPH_PARAM_NAME);

		DatasetImpl dataset = new DatasetImpl();

		if (defaultRemoveGraphURIs != null) {
			for (String graphURI : defaultRemoveGraphURIs) {
				try {
					URI uri = createURIOrNull(repository, graphURI);
					dataset.addDefaultRemoveGraph(uri);
				} catch (IllegalArgumentException e) {
					throw new ClientHTTPException(SC_BAD_REQUEST, "Illegal URI for default remove graph: "
							+ graphURI);
				}
			}
		}

		if (defaultInsertGraphURIs != null && defaultInsertGraphURIs.length > 0) {
			String graphURI = defaultInsertGraphURIs[0];
			try {
				URI uri = createURIOrNull(repository, graphURI);
				dataset.setDefaultInsertGraph(uri);
			} catch (IllegalArgumentException e) {
				throw new ClientHTTPException(SC_BAD_REQUEST, "Illegal URI for default insert graph: " + graphURI);
			}
		}

		if (defaultGraphURIs != null) {
			for (String defaultGraphURI : defaultGraphURIs) {
				try {
					URI uri = createURIOrNull(repository, defaultGraphURI);
					dataset.addDefaultGraph(uri);
				} catch (IllegalArgumentException e) {
					throw new ClientHTTPException(SC_BAD_REQUEST, "Illegal URI for default graph: "
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
					throw new ClientHTTPException(SC_BAD_REQUEST, "Illegal URI for named graph: " + namedGraphURI);
				}
			}
		}

		try {

			RepositoryConnection repositoryCon = repository.getConnection();
			synchronized (repositoryCon) {
				Update update = repositoryCon.prepareUpdate(queryLn, sparqlUpdateString, baseURI);

				update.setIncludeInferred(includeInferred);

				if (dataset != null) {
					update.setDataset(dataset);
				}

				// determine if any variable bindings have been set on this update.
				@SuppressWarnings("unchecked")
				Enumeration<String> parameterNames = request.getParameterNames();

				while (parameterNames.hasMoreElements()) {
					String parameterName = parameterNames.nextElement();

					if (parameterName.startsWith(BINDING_PREFIX)
							&& parameterName.length() > BINDING_PREFIX.length()) {
						String bindingName = parameterName.substring(BINDING_PREFIX.length());
						Value bindingValue = ProtocolUtil.parseValueParam(request, parameterName,
								repository.getValueFactory());
						update.setBinding(bindingName, bindingValue);
					}
				}

				update.execute();
			}
			repositoryCon.close();
			return new ModelAndView(EmptySuccessView.getInstance());
		} catch (UpdateExecutionException e) {
			if (e.getCause() != null && e.getCause() instanceof HTTPException) {
				// custom signal from the backend, throw as HTTPException directly
				// (see SES-1016).
				throw (HTTPException) e.getCause();
			} else {
				throw new ServerHTTPException("Repository update error: " + e.getMessage(), e);
			}
		} catch (RepositoryException e) {
			if (e.getCause() != null && e.getCause() instanceof HTTPException) {
				// custom signal from the backend, throw as HTTPException directly
				// (see SES-1016).
				throw (HTTPException) e.getCause();
			} else {
				throw new ServerHTTPException("Repository update error: " + e.getMessage(), e);
			}
		} catch (MalformedQueryException e) {
			ErrorInfo errInfo = new ErrorInfo(ErrorType.MALFORMED_QUERY, e.getMessage());
			throw new ClientHTTPException(SC_BAD_REQUEST, errInfo.toString());
		}
	}

	/**
	 * greate a graphURI
	 * 
	 * @param repository the Repository object
	 * @param graphURI String of the graphURI
	 * @return the URI of the input graphURI
	 */
	private URI createURIOrNull(final Repository repository, final String graphURI) {
		if ("null".equals(graphURI)) {
			return null;
		}
		return repository.getValueFactory().createURI(graphURI);
	}

	/**
	 * Get all statements and export them as RDF.
	 * 
	 * @param repository the Repository object
	 * @param request the HttpServletRequest object
	 * @param response the HttpServletResponse object
	 * @return a model and view for exporting the statements.
	 * @throws ClientHTTPException throws when there errors in parsing the request
	 */
	private ModelAndView getExportStatementsResult(final Repository repository, final HttpServletRequest request,
			final HttpServletResponse response)
			throws ClientHTTPException {
		ProtocolUtil.logRequestParameters(request);

		ValueFactory vf = repository.getValueFactory();

		Resource subj = ProtocolUtil.parseResourceParam(request, SUBJECT_PARAM_NAME, vf);
		URI pred = ProtocolUtil.parseURIParam(request, PREDICATE_PARAM_NAME, vf);
		Value obj = ProtocolUtil.parseValueParam(request, OBJECT_PARAM_NAME, vf);
		Resource[] contexts = ProtocolUtil.parseContextParam(request, CONTEXT_PARAM_NAME, vf);
		boolean useInferencing = ProtocolUtil.parseBooleanParam(request, INCLUDE_INFERRED_PARAM_NAME, true);

		RDFWriterFactory rdfWriterFactory = ProtocolUtil.getAcceptableService(request, response,
				RDFWriterRegistry.getInstance());

		Map<String, Object> model = new HashMap<String, Object>();
		model.put(ExportStatementsView.SUBJECT_KEY, subj);
		model.put(ExportStatementsView.PREDICATE_KEY, pred);
		model.put(ExportStatementsView.OBJECT_KEY, obj);
		model.put(ExportStatementsView.CONTEXTS_KEY, contexts);
		model.put(ExportStatementsView.USE_INFERENCING_KEY, Boolean.valueOf(useInferencing));
		model.put(ExportStatementsView.FACTORY_KEY, rdfWriterFactory);
		model.put(ExportStatementsView.HEADERS_ONLY, METHOD_HEAD.equals(request.getMethod()));
		model.put(ExportStatementsView.REPO_KEY, repository);
		return new ModelAndView(ExportStatementsView.getInstance(), model);
	}

	/**
	 * Process several actions as a transaction.
	 * 
	 * @param repository the Repository object
	 * @param request the HttpServletRequest object
	 * @param response the HttpServetResponse object
	 * @return the EmptySuccessView if success
	 * @throws IOException throws when there are errors in getting inputstream from the request
	 * @throws HTTPException throws when there are repository update errors
	 */
	private ModelAndView getTransactionResultResult(final Repository repository, final HttpServletRequest request,
			final HttpServletResponse response)
			throws IOException, HTTPException {
		InputStream in = request.getInputStream();
		try {
			_logger.debug("Processing transaction...");
			RepositoryConnection conn = repository.getConnection();
			TransactionReader reader = new TransactionReader();
			Iterable<? extends TransactionOperation> txn = reader.parse(in);

			RepositoryConnection repositoryCon = repository.getConnection();
			synchronized (repositoryCon) {
				repositoryCon.begin();

				for (TransactionOperation op : txn) {
					op.execute(repositoryCon);
				}

				repositoryCon.commit();
			}
			_logger.debug("Transaction processed ");
			repositoryCon.close();
			return new ModelAndView(EmptySuccessView.getInstance());
		} catch (SAXParseException e) {
			ErrorInfo errInfo = new ErrorInfo(ErrorType.MALFORMED_DATA, e.getMessage());
			throw new ClientHTTPException(SC_BAD_REQUEST, errInfo.toString());
		} catch (SAXException e) {
			throw new ServerHTTPException("Failed to parse transaction data: " + e.getMessage(), e);
		} catch (IOException e) {
			throw new ServerHTTPException("Failed to read data: " + e.getMessage(), e);
		} catch (RepositoryException e) {
			if (e.getCause() != null && e.getCause() instanceof HTTPException) {
				// custom signal from the backend, throw as HTTPException directly
				// (see SES-1016).
				throw (HTTPException) e.getCause();
			} else {
				throw new ServerHTTPException("Repository update error: " + e.getMessage(), e);
			}
		}
	}

	/**
	 * Upload data to the repository.
	 * 
	 * @param repository the Repository object
	 * @param request the HttpServletRequest object
	 * @param response the HttpServletReponse object
	 * @param replaceCurrent indicate if replace the original data. set replaceCurrent to true if allowed the original data to be replaced
	 * @return EmptySuccessView if success
	 * @throws IOException throws when there are errors in getting inputstream from the request
	 * @throws HTTPException throws when there are repository update errors
	 */
	private ModelAndView getAddDataResult(final Repository repository, final HttpServletRequest request,
			final HttpServletResponse response, final boolean replaceCurrent)
			throws IOException, HTTPException {
		ProtocolUtil.logRequestParameters(request);

		String mimeType = HttpServerUtil.getMIMEType(request.getContentType());

		RDFFormat rdfFormat = Rio.getParserFormatForMIMEType(mimeType);
		if (rdfFormat == null) {
			throw new ClientHTTPException(SC_UNSUPPORTED_MEDIA_TYPE, "Unsupported MIME type: " + mimeType);
		}

		ValueFactory vf = repository.getValueFactory();

		Resource[] contexts = ProtocolUtil.parseContextParam(request, CONTEXT_PARAM_NAME, vf);
		URI baseURI = ProtocolUtil.parseURIParam(request, BASEURI_PARAM_NAME, vf);

		if (baseURI == null) {
			baseURI = vf.createURI("foo:bar");
			_logger.info("no base URI specified, using dummy '{}'", baseURI);
		}

		InputStream in = request.getInputStream();
		try {
			RepositoryConnection repositoryCon = repository.getConnection();
			synchronized (repositoryCon) {
				if (repositoryCon.isAutoCommit()) {
					repositoryCon.begin();
				}

				if (replaceCurrent) {
					repositoryCon.clear(contexts);
				}
				repositoryCon.add(in, baseURI.toString(), rdfFormat, contexts);

				repositoryCon.commit();
			}
			repositoryCon.close();
			return new ModelAndView(EmptySuccessView.getInstance());
		} catch (UnsupportedRDFormatException e) {
			throw new ClientHTTPException(SC_UNSUPPORTED_MEDIA_TYPE, "No RDF parser available for format "
					+ rdfFormat.getName());
		} catch (RDFParseException e) {
			ErrorInfo errInfo = new ErrorInfo(ErrorType.MALFORMED_DATA, e.getMessage());
			throw new ClientHTTPException(SC_BAD_REQUEST, errInfo.toString());
		} catch (IOException e) {
			throw new ServerHTTPException("Failed to read data: " + e.getMessage(), e);
		} catch (RepositoryException e) {
			if (e.getCause() != null && e.getCause() instanceof HTTPException) {
				// custom signal from the backend, throw as HTTPException directly
				// (see SES-1016).
				throw (HTTPException) e.getCause();
			} else {
				throw new ServerHTTPException("Repository update error: " + e.getMessage(), e);
			}
		}
	}

	/**
	 * Delete data from the repository.
	 * 
	 * @param repository the Repository object
	 * @param request the HttpServletRequest object
	 * @param response the HttpServletResponse object
	 * @return EmptySuccessView if success
	 * @throws HTTPException throws when there are repository update errors
	 */
	private ModelAndView getDeleteDataResult(final Repository repository, final HttpServletRequest request,
			final HttpServletResponse response)
			throws HTTPException {
		ProtocolUtil.logRequestParameters(request);

		ValueFactory vf = repository.getValueFactory();

		Resource subj = ProtocolUtil.parseResourceParam(request, SUBJECT_PARAM_NAME, vf);
		URI pred = ProtocolUtil.parseURIParam(request, PREDICATE_PARAM_NAME, vf);
		Value obj = ProtocolUtil.parseValueParam(request, OBJECT_PARAM_NAME, vf);
		Resource[] contexts = ProtocolUtil.parseContextParam(request, CONTEXT_PARAM_NAME, vf);

		try {
			RepositoryConnection repositoryCon = repository.getConnection();
			synchronized (repositoryCon) {
				repositoryCon.remove(subj, pred, obj, contexts);
			}
			repositoryCon.close();
			return new ModelAndView(EmptySuccessView.getInstance());
		} catch (RepositoryException e) {
			if (e.getCause() != null && e.getCause() instanceof HTTPException) {
				// custom signal from the backend, throw as HTTPException directly
				// (see SES-1016).
				throw (HTTPException) e.getCause();
			} else {
				throw new ServerHTTPException("Repository update error: " + e.getMessage(), e);
			}
		}
	}

}
