package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.util.Iterator;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import edu.kit.aifb.cumulus.framework.Environment;
import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.CumulusStoreException;
import edu.kit.aifb.cumulus.store.Store;

import static edu.kit.aifb.cumulus.webapp.HttpProtocol.*;
import static edu.kit.aifb.cumulus.util.Util.toResultIterator;

/**
 * 
 * Servlet that allows read/write access for Linked Data resources.
 * Note, content negotiation is used for determining the input/output RDF serialization.
 * 
 * @see <a href="http://www.w3.org/DesignIssues/LinkedData.html">http://www.w3.org/DesignIssues/LinkedData.html</a>
 * @see <a href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec12.html">http://www.w3.org/Protocols/rfc2616/rfc2616-sec12.html</a>
 * 
 * @author Steffen Stadtmueller
 * @author Andreas Wagner
 * @since 1.0
 * 
 */
public class LinkedDataServlet extends AbstractCumulusServlet {

	private static final long serialVersionUID = 1L;
	private Log _log = new Log(LoggerFactory.getLogger(getClass()));
	private ValueFactory _valueFactory;

	/**
	 * <p> Deletes all triples associated with the specific entity. That is, an HTTP DELETE request
	 * on the URI {@code http://example.org/resource/1} would delete the triples:</p>
	 * <br>
	 * <p>
	 * http://http://example.org/resource/1 ?p ?o . <br>
	 * ?s ?p http://http://example.org/resource/1 .
	 * </p>
	 * 
	 */
	@Override
	public void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		try {

			final Store store = ((Store) getServletContext().getAttribute(ConfigParams.STORE));

			if ((store == null) || !store.isOpen()) {
				_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG + " Store was null or not initialized.");
				sendError(
						req,
						resp,
						HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
						MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG);
				return;
			}

			final Iterator<Statement> removeIterator = store.describe(ValueFactoryImpl.getInstance().createURI(req.getRequestURL().toString()), false);

			if (removeIterator.hasNext()) {
				store.removeData(removeIterator);
				resp.setStatus(HttpServletResponse.SC_OK);
			} else {
				resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
			}

		} catch (Exception exception) {
			_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE, exception);
			sendError(req, resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG, exception);
		}
	}

	/**
	 * <p> Retrieves all triples associated with the specific entity. That is, an HTTP GET request
	 * on the URI {@code http://example.org/resource/1} would retrieve the triples:</p>
	 * <br>
	 * <p>
	 * http://http://example.org/resource/1 ?p ?o . <br>
	 * ?s ?p http://http://example.org/resource/1 .
	 * </p>
	 * <br>
	 * <p>HTTP header 'Accept' is used to determine the MIME type for the RDF serialization.</p>
	 * 
	 */
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		final RDFFormat format = Rio.getWriterFormatForMIMEType(parseAcceptHeader(req));

		if (format == null) {
			_log.debug(MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID + " Mime-type '" + parseAcceptHeader(req)
					+ "' not supported.");
			sendError(req, resp, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID + " Mime-type '" + parseAcceptHeader(req)
					+ "' not supported.");
			return;
		}

		SailRepositoryConnection connection = null;
		RepositoryResult<Statement> repo_res1 = null, repo_res2 = null;

		try {

			SailRepository repository = (SailRepository) getServletContext().getAttribute(ConfigParams.SESAME_REPO);

			if ((repository == null) || !repository.isInitialized()) {
				_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG + " Repository was null or not initialized.");
				sendError(
						req,
						resp,
						HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
						MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG);
				return;
			}

			connection = repository.getConnection();
			URI entity = _valueFactory.createURI(req.getRequestURL().toString());

			final Iterator<Statement> res1 = toResultIterator(repo_res1 = connection.getStatements(entity, null, null, false));
			final Iterator<Statement> res2 = toResultIterator(repo_res2 = connection.getStatements(null, null, entity, false));

			if (!res1.hasNext() && !res2.hasNext()) {

				sendError(req, resp, HttpServletResponse.SC_NOT_FOUND, MessageCatalog._00033_RESOURCE_NOT_FOUND_MSG);

			} else {

				resp.setCharacterEncoding(Environment.CHARSET_UTF8.name());
				resp.setContentType(format.getDefaultMIMEType());
				resp.setStatus(HttpServletResponse.SC_OK);

				RDFWriter writer = Rio.createWriter(format, resp.getOutputStream());
				Rio.write(new Iterable<Statement>() {

					@Override
					public Iterator<Statement> iterator() {
						return Iterators.concat(res1, res2);
					}
				}, writer);
			}

		} catch (Exception exception) {

			_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE, exception);
			sendError(req, resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG, exception);

		} finally {

			try {

				if (repo_res1 != null) {
					repo_res1.close();
				}

				if (repo_res2 != null) {
					repo_res2.close();
				}

				if (connection != null) {
					connection.close();
				}

			} catch (Exception exception) {
				_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE, exception);
				sendError(req, resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG, exception);
			}
		}
	}

	/**
	 * <p> Posts triples associated with the specific entity.</p>
	 * <p> HTTP header 'Content-Type' is used to determine the MIME type for the RDF serialization.</p>
	 */
	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		RDFFormat format = Rio.getWriterFormatForMIMEType(parseContentTypeHeader(req));

		if (format == null) {

			_log.debug(MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID + " Content-type '" + parseContentTypeHeader(req) + "' not supported.");
			sendError(req, resp, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID + " Content '" + parseContentTypeHeader(req)
					+ "' not supported.");
			return;
		}

		try {

			final Store crdf = ((Store) getServletContext().getAttribute(ConfigParams.STORE));

			if ((crdf == null) || !crdf.isOpen()) {

				_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG + " Store was null or not initialized.");
				sendError(
						req,
						resp,
						HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
						MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG);
				return;
			}

			RDFParser parser = Rio.createParser(format);
			parser.setRDFHandler(new RDFHandler() {

				@Override
				public void endRDF() throws RDFHandlerException {

				}

				@Override
				public void handleComment(String comment) throws RDFHandlerException {

				}

				@Override
				public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
				}

				@Override
				public void handleStatement(Statement st) throws RDFHandlerException {

					try {

						crdf.addData(st);

					} catch (CumulusStoreException e) {
						_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE, e);
					}
				}

				@Override
				public void startRDF() throws RDFHandlerException {
				}
			});

			/*
			 * this allows proper handling of RuntimeExceptions ... 
			 */
			try {

				parser.parse(req.getInputStream(), parseBaseURI(req));

			} catch (RuntimeException exception) {
				throw new RDFParseException(exception);
			}

			resp.setStatus(HttpServletResponse.SC_CREATED);

		} catch (RDFParseException exception) {
			_log.debug(MessageCatalog._00029_RDF_PARSE_FAILURE, exception);
			sendError(req, resp, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00029_RDF_PARSE_FAILURE, exception);
		} catch (RDFHandlerException exception) {
			_log.debug(MessageCatalog._00029_RDF_PARSE_FAILURE, exception);
			sendError(req, resp, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00029_RDF_PARSE_FAILURE, exception);
		} catch (IOException exception) {
			_log.debug(MessageCatalog._00029_RDF_PARSE_FAILURE, exception);
			sendError(req, resp, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00029_RDF_PARSE_FAILURE, exception);
		} catch (Exception exception) {
			_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE, exception);
			sendError(req, resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG, exception);
		}
	}

	/**
	 * Request is forwarded to {@link LinkedDataServlet#doPost(HttpServletRequest, HttpServletResponse)}. 
	 */
	@Override
	public void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		doPost(req, resp);
	}

	@Override
	public void init() throws ServletException {
		_valueFactory = ValueFactoryImpl.getInstance();
	}
}