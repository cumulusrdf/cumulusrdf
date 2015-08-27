package edu.kit.aifb.cumulus.webapp;

import static edu.kit.aifb.cumulus.framework.util.Strings.isNullOrEmptyString;
import static edu.kit.aifb.cumulus.webapp.HttpProtocol.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Locale;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResult;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;

import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.CumulusStoreException;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.MimeTypes;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Parameters;

/**
 * Servlet that executes <a href="http://www.w3.org/TR/sparql11-overview/">SPARQL 1.1</a> updates and queries.
 * 
 * @see {@link edu.kit.aifb.cumulus.webapp.HttpProtocol.Parameters}
 * @see  <a href="http://www.w3.org/TR/sparql11-overview/">http://www.w3.org/TR/sparql11-overview/</a>
 *  
 * @param query - HTTP parameter 'query', which holds a SPARQL query.
 * @param update - HTTP parameter 'update', which holds a SPARQL update.
 * @param base-uri - HTTP parameter 'base-uri', which is specifies the base URI used for SPARQL updates. 
 * 
 * @author Andreas Harth
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * 
 * @since 0.6
 */
public class SPARQLServlet extends AbstractCumulusServlet {

	private static final long serialVersionUID = -8252614862256454962L;

	private ThreadLocal<SimpleDateFormat> _rfc822Formatters = new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			final SimpleDateFormat rfc822 = new SimpleDateFormat("EEE', 'dd' 'MMM' 'yyyy' 'HH:mm:ss' 'Z", Locale.US);
			rfc822.setLenient(false);
			return rfc822;
		}
	};

	@Override
	public void service(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {

		final String accept = parseAcceptHeader(request);
		final String query = getParameterValue(request, Parameters.QUERY), update = getParameterValue(request, Parameters.UPDATE);

		if (isNullOrEmptyString(query) && isNullOrEmptyString(update)) {

			sendError(
					request,
					response,
					HttpServletResponse.SC_BAD_REQUEST,
					MessageCatalog._00045_MISSING_QUERY_OR_UPDATE_PARAM);
			return;
		}

		RepositoryConnection connection = null;

		@SuppressWarnings("rawtypes")
		QueryResult resultset = null;

		try {

			final SailRepository repository = (SailRepository) getServletContext().getAttribute(ConfigParams.SESAME_REPO);

			if ((repository == null) || !repository.isInitialized()) {
				
				_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG + " Repository was null or not initialized.");
				sendError(
						request,
						response,
						HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
						MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG);	
				return;
			}

			connection = repository.getConnection();

			if (query != null) {

				final Query parsedQuery = connection.prepareQuery(QueryLanguage.SPARQL, query, parseBaseURI(request, null));

				if (accept.equals(MimeTypes.TEXT_HTML)) {

					if (parsedQuery instanceof BooleanQuery) {

						request.setAttribute("isThereAResult", true);
						request.setAttribute("booleanQuery", true);
						request.setAttribute("query", query);
						request.setAttribute("result", ((BooleanQuery) parsedQuery).evaluate());
						forwardTo(request, response, "query.vm");

					} else if (parsedQuery instanceof TupleQuery) {

						resultset = ((TupleQuery) parsedQuery).evaluate();
						final TupleQueryResult res = (TupleQueryResult) resultset;
						request.setAttribute("tupleQuery", true);
						request.setAttribute("query", query);

						if (resultset.hasNext()) {

							request.setAttribute("isThereAResult", true);
							request.setAttribute("tupleResult", resultset);
							request.setAttribute("wrappedResult", new Iterator<BindingSet>() {

								@Override
								public boolean hasNext() {
									try {
										return res.hasNext();
									} catch (Exception exception) {
										return false;
									}
								}

								@Override
								public BindingSet next() {
									try {
										return res.next();
									} catch (Exception exception) {
										return null;
									}
								}

								@Override
								public void remove() {
									// Nothing to be done...
								}

							});
						}

						forwardTo(request, response, "query.vm");

					} else if (parsedQuery instanceof GraphQuery) {

						resultset = ((GraphQuery) parsedQuery).evaluate();
						request.setAttribute("graphQuery", true);
						request.setAttribute("query", query);

						if (resultset.hasNext()) {

							final GraphQueryResult res = (GraphQueryResult) resultset;
							request.setAttribute("isThereAResult", true);
							request.setAttribute("graphResult", resultset);
							request.setAttribute("wrappedResult", new Iterator<Statement>() {

								@Override
								public boolean hasNext() {
									try {
										return res.hasNext();
									} catch (Exception exception) {
										return false;
									}
								}

								@Override
								public Statement next() {
									try {
										return res.next();
									} catch (Exception exception) {
										return null;
									}
								}

								@Override
								public void remove() {
									// Nothing to be done...
								}
							});

						}

						forwardTo(request, response, "query.vm");
					}

				} else {

					final Calendar c = Calendar.getInstance();
					c.add(Calendar.DATE, 7);

					response.setHeader("Cache-Control", "public");
					response.setHeader("Expires", _rfc822Formatters.get().format(c.getTime()));
					response.setHeader("Access-Control-Allow-Origin", "*");
					response.setHeader("Vary", "Accept");

					if (parsedQuery instanceof BooleanQuery) {

						BooleanQueryResultFormat format = BooleanQueryResultFormat.forMIMEType(accept, BooleanQueryResultFormat.SPARQL);
						response.setContentType(format.getDefaultMIMEType());

						QueryResultIO.writeBoolean(
								((BooleanQuery) parsedQuery).evaluate(),
								format,
								response.getOutputStream());

					} else if (parsedQuery instanceof TupleQuery) {

						TupleQueryResultFormat format = TupleQueryResultFormat.forMIMEType(accept, TupleQueryResultFormat.SPARQL);
						response.setContentType(format.getDefaultMIMEType());

						resultset = ((TupleQuery) parsedQuery).evaluate();

						QueryResultIO.write(
								(TupleQueryResult) resultset,
								format,
								response.getOutputStream());

					} else if (parsedQuery instanceof GraphQuery) {

						RDFFormat format = RDFFormat.forMIMEType(accept, RDFFormat.RDFXML);
						response.setContentType(format.getDefaultMIMEType());

						resultset = ((GraphQuery) parsedQuery).evaluate();

						QueryResultIO.write(
								(GraphQueryResult) resultset,
								format,
								response.getOutputStream());
					}

					response.setStatus(HttpServletResponse.SC_OK);
				}
			} else if (update != null) {

				connection.prepareUpdate(QueryLanguage.SPARQL, update, parseBaseURI(request)).execute();
				response.setStatus(HttpServletResponse.SC_OK);

			}
		} catch (final QueryResultHandlerException e) {
			_log.debug(MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID, e);
			sendError(request, response, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID, e);
		} catch (final RDFHandlerException e) {
			_log.debug(MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID, e);
			sendError(request, response, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID, e);
		} catch (final IOException e) {
			_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG, e);
			sendError(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG, e);
		} catch (QueryEvaluationException e) {
			_log.debug(MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID, e);
			sendError(request, response, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID, e);
		} catch (RepositoryException e) {
			_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG, e);
			sendError(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG, e);
		} catch (MalformedQueryException e) {
			_log.debug(MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID, e);
			sendError(request, response, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID, e);
		} catch (UpdateExecutionException e) {
			_log.debug(MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID, e);
			sendError(request, response, HttpServletResponse.SC_BAD_REQUEST, MessageCatalog._00115_WEB_MODULE_REQUEST_NOT_VALID, e);
		} catch (final CumulusStoreException e) {
			_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE, e);
			sendError(
					request,
					response,
					HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
					MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG,
					e);
		} catch (final Exception e) {
			_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE, e);
			e.printStackTrace();
			sendError(
					request,
					response,
					HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
					MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE,
					e);
		} finally {
			// CHECKSTYLE:OFF
			// @formatter:off
			if (resultset != null) { try { resultset.close();} catch (final Exception ignore) {}};
			if (connection != null) { try { connection.close();} catch (final Exception ignore) {}};
			// @formatter:on
			// CHECKSTYLE:ON
		}
	}
}