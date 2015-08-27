package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Headers;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.MimeTypes;

/**
 * A servlet that displays the current status of the connected Cassandra
 * cluster. That is the status of every pool of the cluster, read/write
 * statistics, information about the schema and the number of active
 * additions/deletions.
 * 
 * @author Andreas Harth
 * @author Andrea Gazzarini
 */
public class InfoServlet extends AbstractCumulusServlet {

	private static final long serialVersionUID = -326095061705259451L;

	@SuppressWarnings("unchecked")
	@Override
	public void service(final HttpServletRequest request, final HttpServletResponse response) throws IOException, ServletException {
		
		String accept = request.getHeader(Headers.ACCEPT) == null ? "text/plain" : request.getHeader(Headers.ACCEPT);
		
		if (accept.contains("html")) {
			
			request.setAttribute("page", "Overview");
			forwardTo(request, response, "info.vm");
			
		} else {

			PrintWriter out = response.getWriter();
			response.setContentType(MimeTypes.TEXT_PLAIN);

			ServletContext ctx = getServletContext();
			Store crdf = (Store) getServletContext().getAttribute(ConfigParams.STORE);

			out.println("Status: " + crdf.getStatus());

			for (Enumeration<String> e = ctx.getAttributeNames(); e.hasMoreElements();) {
				String attr = e.nextElement();
				out.println("Setting: attribute = " + attr + " @ value = " + ctx.getAttribute(attr));
			}

			out.close();
		}
	}
}