package edu.kit.aifb.cumulus.webapp.gui;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.kit.aifb.cumulus.webapp.AbstractCumulusServlet;

/**
 * A servlet that handles information messages about a feature that hasn't been implemented.
 * 
 * @author Andrea Gazzarini
 */
public class FeatureNotYetImplementedServlet extends AbstractCumulusServlet {

	private static final long serialVersionUID = -326095061705259451L;

	@Override
	public void service(final HttpServletRequest request, final HttpServletResponse response) throws IOException, ServletException {
		final String accept = request.getHeader("accept") == null ? "text" : request.getHeader("accept");
		if (accept.contains("html")) {
			request.setAttribute("page", "Feature Not Yet Implemented");
			forwardTo(request, response, "nyi.vm");
		} else {
			response.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED, "Feature Not Yet Implemented");
		}
	}
}