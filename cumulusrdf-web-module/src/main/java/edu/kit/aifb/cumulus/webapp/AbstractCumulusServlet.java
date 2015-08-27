package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.MimeTypes;

/**
 * Supertype layer for all CumulusRDF servlets.
 * 
 * @author Andreas Harth
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class AbstractCumulusServlet extends HttpServlet {

	private static final long serialVersionUID = 8288745695008790134L;

	protected final Log _log = new Log(LoggerFactory.getLogger(getClass()));

	private static final String PARAMS_MISSING_HTML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\" "
			+ "\"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\"> <html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\" lang=\"en\"> "
			+ "<head> <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\" /> <title>CumulusRDF - Paramters Missing</title> "
			+ "</head><body><h1>CumulusRDF - Paramters Missing</h1>";

	@Override
	public void doDelete(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
		resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
	}

	@Override
	public void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
		resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
	}

	@Override
	public void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
		resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
	}

	@Override
	public void doPut(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
		resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
	}

	/**
	 * Forwards to a given target resource.
	 * 
	 * @param request the HTTP request.
	 * @param response the HTTP response.
	 * @param targetResource the target resource.
	 * @throws ServletException in case of Servlet I/O failure.
	 * @throws IOException in case of I/O failure.
	 */
	protected void forwardTo(
			final HttpServletRequest request,
			final HttpServletResponse response,
			final String targetResource) throws ServletException, IOException {

		RequestDispatcher dispatcher = request.getRequestDispatcher(targetResource);
		dispatcher.forward(request, response);
	}

	/**
	 * Redirects to a given target resource.
	 * 
	 * @param request the HTTP request.
	 * @param response the HTTP response.
	 * @param targetResource the target resource.
	 * @throws IOException in case of I/O failure.
	 */
	protected void redirectTo(
			final HttpServletRequest request,
			final HttpServletResponse response,
			final String targetResource) throws IOException {
		response.sendRedirect(targetResource);
	}

	private String getParametersMissingHTML(final String... params) {

		final StringBuilder html = new StringBuilder(PARAMS_MISSING_HTML);

		if ((params == null) || (params.length == 0)) {
			return html.append("<p>No parameters provided.</p></body></html>").toString();
		}

		for (final String param : params) {
			html.append("<p>No parameter " + param + " provided.</p>");
		}

		return html.append("</body></html>").toString();
	}

	/**
	 * Sends back an error status / message.
	 * 
	 * @param req the Http request.
	 * @param resp the Http response.
	 * @param statusCode the Htttp status code.
	 * @param msg an explanation message.
	 * @param cause one or more exception causes.
	 */
	protected void sendError(
			final HttpServletRequest req,
			final HttpServletResponse resp,
			final int statusCode,
			final String msg,
			final Throwable... cause) {

		resp.setStatus(statusCode);
		req.setAttribute("javax.servlet.error.status_code", statusCode);
		req.setAttribute("javax.servlet.error.message", msg);
		req.setAttribute("javax.servlet.error.request_uri", req.getRequestURI());
		req.setAttribute("page", "Error");

		if ((cause != null) && (cause.length > 0)) {			
			req.setAttribute("javax.servlet.error.exception", cause);
		}

		try {
			forwardTo(req, resp, "/error");
		} catch (ServletException e) {
			_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG, e);
		} catch (IOException e) {
			_log.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG, e);
		}
	}

	/**
	 * Returns a HTML page that includes all missing parameters.
	 * 
	 * @param resp the HTTP response.
	 * @param params the missing parameters.
	 * @throws IOException in case of I/O failure.
	 */
	protected void sendNiceHTMLParameterMissingError(final HttpServletResponse resp, final String... params) throws IOException {
		resp.setContentType(MimeTypes.TEXT_HTML);
		PrintWriter writer = resp.getWriter();
		writer.write(getParametersMissingHTML(params));
		writer.close();
		resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
	}
}