package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.kit.aifb.cumulus.log.MessageCatalog;
import static edu.kit.aifb.cumulus.webapp.HttpProtocol.*;

/**
 * Servlet for displaying error messages.
 * 
 * @author Andreas Harth
 * @author Andrea Gazzarini
 * @since 0.6
 * 
 */
@SuppressWarnings("serial")
public class ErrorServlet extends AbstractCumulusServlet {

	@Override
	public void service(final HttpServletRequest req, final HttpServletResponse resp) throws IOException, ServletException {

		String accept = parseAcceptHeader(req), code = null, message = null, type = null, uri = null;
		Object codeObj, messageObj, typeObj;

		// Retrieve the three possible error attributes, some may be null
		codeObj = req.getAttribute("javax.servlet.error.status_code");
		messageObj = req.getAttribute("javax.servlet.error.message");
		typeObj = req.getAttribute("javax.servlet.error.exception_type");
		uri = (String) req.getAttribute("javax.servlet.error.request_uri");

		if (uri == null) {
			uri = req.getRequestURI(); // in case there's no URI given
		}

		// Convert the attributes to string values
		// We do things this way because some old servers return String
		// types while new servers return Integer, String, and Class types.
		// This works for all.
		if (codeObj != null) {
			code = codeObj.toString();
		}
		
		if (messageObj != null) {
			message = messageObj.toString();
		}
		
		if (typeObj != null) {
			type = typeObj.toString();
		}

		// The error reason is either the status code or exception type
		String reason = (code != null ? code : type);
		
		if (accept.equals(MimeTypes.TEXT_PLAIN)) {

			resp.setContentType("text/plain;charset=UTF-8");
			PrintWriter out = resp.getWriter();

			out.println("ERROR " + uri + " " + reason + ": " + message);
			out.flush();
			
		} else {
			
			try {
				forwardTo(req, resp, "error.vm");
			} catch (final ServletException exception) {
				_log.error(MessageCatalog._00031_WEB_MODULE_SERVLET_FAILURE, exception);
			} catch (final IOException exception) {
				_log.error(MessageCatalog._00030_WEB_MODULE_IO_FAILURE, exception);
			}
		}
	}
}