package edu.kit.aifb.cumulus.webapp.gui;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.kit.aifb.cumulus.webapp.AbstractCumulusServlet;

/**
 * A servlet that manages the "Query" area of the dashboard.
 * 
 * @author Andrea Gazzarini
 * @since 1.0.1
 */
public class QueryServlet extends AbstractCumulusServlet {

	private static final long serialVersionUID = -326095061705259451L;

	@Override
	public void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException, ServletException {
		request.setAttribute("page", "Query");
		forwardTo(request, response, "query.vm");
	}
}