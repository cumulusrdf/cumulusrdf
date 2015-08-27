package edu.kit.aifb.cumulus.webapp.endpoint.repository.graph;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE;
import info.aduna.webapp.util.HttpServerUtil;
import info.aduna.webapp.views.EmptySuccessView;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.http.protocol.error.ErrorInfo;
import org.openrdf.http.protocol.error.ErrorType;
import org.openrdf.http.server.ClientHTTPException;
import org.openrdf.http.server.ProtocolUtil;
import org.openrdf.http.server.ServerHTTPException;
import org.openrdf.http.server.repository.statements.ExportStatementsView;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
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

import edu.kit.aifb.cumulus.webapp.endpoint.SesameHTTPProtocolHandler;

/**
 * Handles requests for manipulating the named graphs in a repository.
 * 
 * The code is modified based on Sesame's org.openrdf.http.server.repository.graph.GraphController
 * 
 * @author Yongtao
 *
 */
public class GraphHandler extends SesameHTTPProtocolHandler {

	private Logger _logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public ModelAndView serve(final Repository repository, final HttpServletRequest request, final HttpServletResponse response)
			throws Exception {
		ModelAndView result;
		String reqMethod = request.getMethod();

		if (METHOD_GET.equals(reqMethod)) {
			_logger.info("GET graph");
			result = getExportStatementsResult(repository, request, response);
			_logger.info("GET graph request finished.");
		} else if (METHOD_HEAD.equals(reqMethod)) {
			_logger.info("HEAD graph");
			result = getExportStatementsResult(repository, request, response);
			_logger.info("HEAD graph request finished.");
		} else if (METHOD_POST.equals(reqMethod)) {
			_logger.info("POST data to graph");
			result = getAddDataResult(repository, request, response, false);
			_logger.info("POST data request finished.");
		} else if ("PUT".equals(reqMethod)) {
			_logger.info("PUT data in graph");
			result = getAddDataResult(repository, request, response, true);
			_logger.info("PUT data request finished.");
		} else if ("DELETE".equals(reqMethod)) {
			_logger.info("DELETE data from graph");
			result = getDeleteDataResult(repository, request, response);
			_logger.info("DELETE data request finished.");
		} else {
			throw new ClientHTTPException(HttpServletResponse.SC_METHOD_NOT_ALLOWED, "Method not allowed: "
					+ reqMethod);
		}
		return result;
	}

	/**
	 * get the URI of GRAPH, shoulc be "graph"
	 * 
	 * @param request the HttpServletRequest object
	 * @param vf the ValueFactory object
	 * @return the URI of the name of Graph
	 * @throws ClientHTTPException throws when no parameters epxected for direct reference request
	 */
	private URI getGraphName(final HttpServletRequest request, final ValueFactory vf)
			throws ClientHTTPException {
		String requestURL = request.getRequestURL().toString();
		boolean isServiceRequest = requestURL.endsWith("/service");

		String queryString = request.getQueryString();

		if (isServiceRequest) {
			if (!"default".equalsIgnoreCase(queryString)) {
				URI graph = ProtocolUtil.parseGraphParam(request, vf);
				if (graph == null) {
					throw new ClientHTTPException(HttpServletResponse.SC_BAD_REQUEST,
							"Named or default graph expected for indirect reference request.");
				}
				return graph;
			}
			return null;
		} else {
			if (queryString != null) {
				throw new ClientHTTPException(HttpServletResponse.SC_BAD_REQUEST,
						"No parameters epxected for direct reference request.");
			}
			return vf.createURI(requestURL);
		}
	}

	/**
	 * Get all statements and export them as RDF.
	 * 
	 * @param repository the Repository object
	 * @param request the HttpServletRequest object
	 * @param response the HttpServletResponse object
	 * @return a model and view for exporting the statements.
	 * @throws ClientHTTPException throws when errors in parameters 
	 */
	private ModelAndView getExportStatementsResult(final Repository repository, final HttpServletRequest request,
			final HttpServletResponse response)
			throws ClientHTTPException {
		ProtocolUtil.logRequestParameters(request);

		ValueFactory vf = repository.getValueFactory();

		URI graph = getGraphName(request, vf);

		RDFWriterFactory rdfWriterFactory = ProtocolUtil.getAcceptableService(request, response,
				RDFWriterRegistry.getInstance());

		Map<String, Object> model = new HashMap<String, Object>();

		model.put(ExportStatementsView.CONTEXTS_KEY, new Resource[] { graph });
		model.put(ExportStatementsView.FACTORY_KEY, rdfWriterFactory);
		model.put(ExportStatementsView.USE_INFERENCING_KEY, true);
		model.put(ExportStatementsView.HEADERS_ONLY, METHOD_HEAD.equals(request.getMethod()));
		return new ModelAndView(ExportStatementsView.getInstance(), model);
	}

	/**
	 * Upload data to the graph.
	 * 
	 * @param repository the Repository object
	 * @param request the HttpServletRequest object
	 * @param response the HttpServletResponse object
	 * @param replaceCurrent indicate if replace current data
	 * @return the EmptySuccessView if successes
	 * @throws IOException throws when failed to read data
	 * @throws ClientHTTPException throws when Unsupported MIME type or No RDF parser available for format 
	 * @throws ServerHTTPException throws when errors happens update the data
	 */
	private ModelAndView getAddDataResult(final Repository repository, final HttpServletRequest request,
			final HttpServletResponse response, final boolean replaceCurrent)
			throws IOException, ClientHTTPException, ServerHTTPException {
		ProtocolUtil.logRequestParameters(request);

		String mimeType = HttpServerUtil.getMIMEType(request.getContentType());

		RDFFormat rdfFormat = Rio.getParserFormatForMIMEType(mimeType);
		if (rdfFormat == null) {
			throw new ClientHTTPException(SC_UNSUPPORTED_MEDIA_TYPE, "Unsupported MIME type: " + mimeType);
		}

		ValueFactory vf = repository.getValueFactory();

		URI graph = getGraphName(request, vf);

		InputStream in = request.getInputStream();
		try {
			RepositoryConnection repositoryCon = repository.getConnection();
			synchronized (repositoryCon) {
				if (repositoryCon.isAutoCommit()) {
					repositoryCon.begin();
				}

				if (replaceCurrent) {
					repositoryCon.clear(graph);
				}
				repositoryCon.add(in, graph.toString(), rdfFormat, graph);

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
			throw new ServerHTTPException("Repository update error: " + e.getMessage(), e);
		}
	}

	/**
	 * Delete data from the graph.
	 * 
	 * @param repository the Repository object
	 * @param request the HttpServletRequest object
	 * @param response the HttpServletResponse object
	 * @return the EmptySuccessView if successes
	 * @throws ClientHTTPException throws when there are errors in getting the name of the Graph
	 * @throws ServerHTTPException throws when errors happens update the data
	 */
	private ModelAndView getDeleteDataResult(final Repository repository,
			final HttpServletRequest request, final HttpServletResponse response)
			throws ClientHTTPException, ServerHTTPException {
		ProtocolUtil.logRequestParameters(request);

		ValueFactory vf = repository.getValueFactory();

		URI graph = getGraphName(request, vf);

		try {
			RepositoryConnection repositoryCon = repository.getConnection();
			synchronized (repositoryCon) {
				repositoryCon.clear(graph);
			}
			repositoryCon.close();
			return new ModelAndView(EmptySuccessView.getInstance());
		} catch (RepositoryException e) {
			throw new ServerHTTPException("Repository update error: " + e.getMessage(), e);
		}
	}

}
