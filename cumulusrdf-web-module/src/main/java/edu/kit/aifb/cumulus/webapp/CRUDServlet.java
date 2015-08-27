package edu.kit.aifb.cumulus.webapp;

import static edu.kit.aifb.cumulus.framework.util.Strings.isNotNullOrEmptyString;
import static edu.kit.aifb.cumulus.framework.util.Strings.isNullOrEmptyString;
import static edu.kit.aifb.cumulus.util.Util.ALL_CONSTANTS;
import static edu.kit.aifb.cumulus.util.Util.ALL_VARS;
import static edu.kit.aifb.cumulus.util.Util.CONTAINS_VAR;
import static edu.kit.aifb.cumulus.util.Util.singletonIterator;
import static edu.kit.aifb.cumulus.util.Util.toResultIterator;
import static edu.kit.aifb.cumulus.webapp.HttpProtocol.*;
import static edu.kit.aifb.cumulus.webapp.writer.HTMLWriter.HTML_FORMAT;

import java.io.IOException;
import java.util.Iterator;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
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
import org.openrdf.rio.ntriples.NTriplesUtil;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import edu.kit.aifb.cumulus.framework.Environment;
import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.framework.Environment.ConfigValues;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.CumulusStoreException;
import edu.kit.aifb.cumulus.store.Store;

/**
 * <p>
 * 
 * Implements the <a href="http://en.wikipedia.org/wiki/Create,_read,_update_and_delete">CRUD</a> operations using HTTP as operation selector. Parameters
 * must be encoded in N-Triples syntax.
 * 
 * <ul>
 * <li><b>DELETE</b>: delete a triple or pattern. Parameters: s,p,o (c) (if omitted a
 * parameter will be treated as variable).</li>
 * <li><b>GET</b>: Describes a requested URI. Parameters: uri.</li>
 * <li><b>POST</b>: Insert data into the store. All common RDF serializations are supported.</li>
 * <li><b>PUT</b>: Updates a triple. Parameters for the "original" triple: s,p,o (c).
 * Parameters for the "new" triple: and s2,p2,o2 (c2).</li>
 * </ul>
 * </p>
 * 
 * <p>
 * Note, if parameter "c" (i.e., context) is specified, the storage layout must
 * be a "quad".
 * </p>
 *
 * @see <a href="http://code.google.com/p/cumulusrdf/wiki/Webapps">http://code.google.com/p/cumulusrdf/wiki/Webapps</a> 
 * @see <a href="http://www.w3.org/TR/n-triples/">http://www.w3.org/TR/n-triples/</a>
 * @see {@link edu.kit.aifb.cumulus.webapp.HttpProtocol.Parameters}
 * 
 * @author Andreas Harth
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * 
 * @since 1.0
 */
public class CRUDServlet extends AbstractCumulusServlet {

	private static final long serialVersionUID = -2672280063418774760L;

	private Log _log = new Log(LoggerFactory.getLogger(CRUDServlet.class));
	private ValueFactory _valueFactory;

	/**
	 * <p>To delete a triple send a HTTP DELETE request. 
	 * All parameters must be encoded in <a href="http://www.w3.org/TR/n-triples/">N-Triples</a> syntax.</p>
	 * 
	 * <p>The request takes the following parameters:</p>
	 * 
	 * <ul>	
	 *  <li>
	 * 		<b>s, p, o, c</b>: for the subject, predicate and object of the triple to delete. 
	 * 		One or two of the parameters can be left out, which deletes all triples with the given parameters. 
	 * 		For example, if no subject is given, all triples with the given predicate and object are deleted.
	 * 	</li>
	 *  <li>
	 *  	<b>uri</b>: a specific resource. If this is given, all triples having this resource as subject or object are deleted. 
	 * 	</li>
	 * </ul>
	 * 
	 * @param req the HTTP request.
	 * @param resp the HTTP response.
	 * 
	 * @throws IOException in case of I/O failure.
	 * @throws ServletException in case of application failure.
	 * 
	 * @see <a href="http://www.w3.org/TR/n-triples/">http://www.w3.org/TR/n-triples/</a>
	 * @see {@link edu.kit.aifb.cumulus.webapp.HttpProtocol.Parameters}
	 */
	@Override
	public void doDelete(final HttpServletRequest req, final HttpServletResponse resp) throws IOException, ServletException {

		final ServletContext ctx = getServletContext();
		final String uri = getParameterValue(req, Parameters.URI);
		final String s = getParameterValue(req, Parameters.S), p = getParameterValue(req, Parameters.P), o = getParameterValue(req, Parameters.O), c = getParameterValue(req,
				Parameters.C);

		// Sanity check: at least one parameter must be valid.
		if (isNullOrEmptyString(uri) && isNullOrEmptyString(s) && isNullOrEmptyString(p) && isNullOrEmptyString(o) && isNullOrEmptyString(c)) {
			_log.debug(MessageCatalog._00024_MISSING_REQUIRED_PARAM, req.getQueryString());
			sendError(req, resp, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00035_MISSING_URI_OR_PATTERN);
			return;
		}

		final Store crdf = (Store) ctx.getAttribute(ConfigParams.STORE);

		if ((crdf == null) || !crdf.isOpen()) {
			_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG + " Store was null or not initialized.");
			sendError(
					req,
					resp,
					HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
					MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG);
			return;
		}

		try {

			Iterator<Statement> result = null;

			if (isNotNullOrEmptyString(uri)) {

				result = crdf.describe(NTriplesUtil.parseResource(uri, _valueFactory), false);

			} else {

				final String layout = (String) ctx.getAttribute(ConfigParams.LAYOUT);

				final Value node_s = (s != null ? NTriplesUtil.parseResource(s, _valueFactory) : null);
				final Value node_p = (p != null ? NTriplesUtil.parseURI(p, _valueFactory) : null);
				final Value node_o = (o != null ? NTriplesUtil.parseValue(o, _valueFactory) : null);

				if (ConfigValues.STORE_LAYOUT_TRIPLE.equals(layout)) {

					final Value[] pattern = new Value[] { node_s, node_p, node_o };
					result = ALL_CONSTANTS.apply(pattern)
							? singletonIterator(_valueFactory.createStatement((Resource) node_s, (URI) node_p, node_o))
							: crdf.query(pattern);
				} else {

					final Value node_c = (c != null ? _valueFactory.createURI(c) : null);

					if (node_c == null) {

						final Value[] pattern = new Value[] { node_s, node_p, node_o };

						if (ALL_CONSTANTS.apply(pattern)) {
							result = singletonIterator(_valueFactory.createStatement((Resource) node_s, (URI) node_p, node_o));
						} else {
							result = crdf.query(pattern);
						}
					} else {
						result = crdf.query(new Value[] { node_s, node_p, node_o, node_c });
					}
				}
			}

			if ((result != null) && result.hasNext()) {
				crdf.removeData(result);
			} else {
				sendError(req, resp, HttpServletResponse.SC_NOT_FOUND, MessageCatalog._00033_RESOURCE_NOT_FOUND_MSG);
				return;
			}
		} catch (final CumulusStoreException exception) {
			_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE, exception);
			sendError(
					req,
					resp,
					HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
					MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG,
					exception);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			sendError(
					req,
					resp,
					HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
					MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG,
					exception);
		}
	}

	/**
	 * <p>To request triples, send a HTTP GET request. This method returns triples that have the given URL as subject or object. 
	 * The service accepts:</p>
	 * 
	 * <ul>
	 * 	<li>
	 * 		an <b>uri</b> parameter (must be encoded in N-Triples syntax) for the URL that should be subject or object of the returned triples. 
	 * 	</li>
	 * 	<li>
	 * 		an <b>accept</b> header for specifying a given output format (default: RDF/XML).
	 *  </li>
	 * </ul>
	 * 
	 * @param req the HTTP request.
	 * @param resp the HTTP response.
	 * 
	 * @see {@link edu.kit.aifb.cumulus.webapp.HttpProtocol.Parameters}
	 * 
	 * @throws IOException in case of I/O failure.
	 * @throws ServletException in case of application failure.
	 */
	@Override
	public void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws IOException, ServletException {

		final String uri = getParameterValue(req, Parameters.URI);

		if (isNullOrEmptyString(uri)) {
			_log.debug(MessageCatalog._00023_MISSING_URI);
			sendError(req, resp, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00024_MISSING_REQUIRED_PARAM + " Missing URI parameter.");
			return;
		}

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
			Resource entity = NTriplesUtil.parseResource(uri, _valueFactory);

			final Iterator<Statement> res1 = toResultIterator(repo_res1 = connection.getStatements(entity, null, null, false));
			final Iterator<Statement> res2 = toResultIterator(repo_res2 = connection.getStatements(null, null, entity, false));

			if (!res1.hasNext() && !res2.hasNext()) {
				sendError(req, resp, HttpServletResponse.SC_NOT_FOUND, MessageCatalog._00033_RESOURCE_NOT_FOUND_MSG);
			} else {

				if (format.equals(HTML_FORMAT)) {

					req.setAttribute("uri", entity.stringValue());
					req.setAttribute("result", Iterators.concat(res1, res2));
					forwardTo(req, resp, "pattern-query-result.vm");

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
	 * To insert triples send a HTTP POST request.
	 * The content-type header must match the RDF serialization used. All major RDF serializations are supported.
	 * A base URI can be specified in the HTTP header field 'base-uri'. 
	 * 
	 * @param req the HTTP request.
	 * @param resp the HTTP response.
	 * @see {@link edu.kit.aifb.cumulus.webapp.HttpProtocol.Parameters}
	 * @throws IOException in case of I/O failure.
	 * @throws ServletException in case of application failure.
	 */
	@Override
	public void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws IOException, ServletException {

		final ServletContext ctx = getServletContext();
		final String contentType = parseContentTypeHeader(req), baseURI = parseBaseURI(req);

		RDFFormat format = Rio.getParserFormatForMIMEType(contentType);

		if (format == null) {
			_log.debug(MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID + " Content-type '" + contentType + "' not supported.");
			sendError(req, resp, HttpServletResponse.SC_BAD_REQUEST,
					MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID + " Content-type '" + contentType + "' not supported.");
			return;
		}

		final Store crdf = (Store) ctx.getAttribute(ConfigParams.STORE);

		if ((crdf == null) || !crdf.isOpen()) {

			_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG + " Store was null or not initialized.");
			sendError(
					req,
					resp,
					HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
					MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG);
			return;
		}

		try {

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

				parser.parse(req.getInputStream(), baseURI);

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
	 * <p>To update a triple send a HTTP PUT request. 
	 * The parameters can be specified as URL parameters or in the request body using N-Triples syntax. For instance: </p>
	 * 
	 * <p>s=&lt;http://example.org/id/s1&gt;</p>
	 * <p>p=&lt;http://example.org/id/p1&gt;</p>
	 * 
	 *<p> The parameters are: </p>
	 * 
	 * <ul>
	 * 	<li>s, p, o, c: for the subject, predicate and object of the old triple. 
	 * 		If not all values are given, that parameter is treated as variable (and the first triple that is bound is updated). 
	 * 	</li>
	 * 	<li>
	 * 		s2, p2, o2, c2: for the subject, predicate and object that should act as replacement for the old triple. 
	 * 		If some of this parameters is not given, the respective part of the triple remains unchanged. 
	 * 		For example, if no subject is given, only the predicate and object are updated. 
	 * 	</li>
	 * </ul>
	 * 
	 * @param req the HTTP request.
	 * @param resp the HTTP response.
	 * @throws IOException in case of I/O failure.
	 * @throws ServletException in case of application failure.
	 * @see {@link http://www.w3.org/TR/n-triples/}
	 * @see {@link edu.kit.aifb.cumulus.webapp.HttpProtocol.Parameters}
	 * 
	 */
	@Override
	public void doPut(final HttpServletRequest req, final HttpServletResponse resp) throws IOException, ServletException {

		final ServletContext ctx = getServletContext();
		final Store crdf = (Store) ctx.getAttribute(ConfigParams.STORE);

		if ((crdf == null) || !crdf.isOpen()) {
			_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG + " Store was null or not initialized.");
			sendError(
					req,
					resp,
					HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
					MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG);
			return;
		}

		// old triple
		String s = getParameterValue(req, Parameters.S), p = getParameterValue(req, Parameters.P), o = getParameterValue(req, Parameters.O), c = getParameterValue(req,
				Parameters.C);

		// new triple
		String s2 = getParameterValue(req, Parameters.S2), p2 = getParameterValue(req, Parameters.P2), o2 = getParameterValue(req, Parameters.O2), c2 = getParameterValue(req,
				Parameters.C2);

		try {

			boolean quad = ConfigValues.STORE_LAYOUT_QUAD.equalsIgnoreCase((String) ctx.getAttribute(ConfigParams.LAYOUT));

			final Value node_s = (s != null ? NTriplesUtil.parseResource(s, _valueFactory) : null);
			final Value node_p = (p != null ? NTriplesUtil.parseURI(p, _valueFactory) : null);
			final Value node_o = (o != null ? NTriplesUtil.parseValue(o, _valueFactory) : null);
			final Value node_c = (c != null ? NTriplesUtil.parseResource(c, _valueFactory) : null);

			Value[] old_triple = quad ? new Value[] { node_s, node_p, node_o, node_c } : new Value[] { node_s, node_p, node_o };
			Statement firstMatchingTriple = null;

			if (ALL_VARS.apply(old_triple)) {

				sendError(req, resp, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID + " There must be at least one constant.");
				return;

			} else if (CONTAINS_VAR.apply(old_triple)) {

				final Iterator<Statement> iterator = crdf.query(old_triple);

				if (!iterator.hasNext()) {
					sendError(req, resp, HttpServletResponse.SC_NOT_FOUND, MessageCatalog._00033_RESOURCE_NOT_FOUND_MSG);
					return;
				} else {
					firstMatchingTriple = iterator.next();
				}
			} else {
				firstMatchingTriple = quad
						? _valueFactory.createStatement((Resource) node_s, (URI) node_p, node_o, (Resource) node_c)
						: _valueFactory.createStatement((Resource) node_s, (URI) node_p, node_o);
			}

			final Value node_s2 = (s2 != null ? NTriplesUtil.parseResource(s2, _valueFactory) : firstMatchingTriple.getSubject());
			final Value node_p2 = (p2 != null ? NTriplesUtil.parseURI(p2, _valueFactory) : firstMatchingTriple.getPredicate());
			final Value node_o2 = (o2 != null ? NTriplesUtil.parseValue(o2, _valueFactory) : firstMatchingTriple.getObject());
			final Value node_c2 = (c2 != null ? NTriplesUtil.parseResource(c2, _valueFactory) : (quad ? firstMatchingTriple.getContext() : null));

			crdf.removeData(quad
					? new Value[] {
							firstMatchingTriple.getSubject(),
							firstMatchingTriple.getPredicate(),
							firstMatchingTriple.getObject(),
							firstMatchingTriple.getContext() }
					: new Value[] {
							firstMatchingTriple.getSubject(),
							firstMatchingTriple.getPredicate(),
							firstMatchingTriple.getObject() });

			crdf.addData(quad
						? _valueFactory.createStatement((Resource) node_s2, (URI) node_p2, node_o2, (Resource) node_c2)
						: _valueFactory.createStatement((Resource) node_s2, (URI) node_p2, node_o2));

		} catch (final CumulusStoreException exception) {

			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			sendError(req, resp, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID,
					exception);

		} catch (final Exception exception) {

			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			sendError(req, resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG,
					exception);
		}
	}

	@Override
	public void init() {
		_valueFactory = ValueFactoryImpl.getInstance();
	}
}