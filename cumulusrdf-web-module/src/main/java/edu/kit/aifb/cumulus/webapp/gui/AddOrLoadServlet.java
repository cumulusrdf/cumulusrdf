package edu.kit.aifb.cumulus.webapp.gui;

import static edu.kit.aifb.cumulus.framework.util.Strings.isNullOrEmptyString;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import edu.kit.aifb.cumulus.webapp.AbstractCumulusServlet;
import edu.kit.aifb.cumulus.webapp.HttpProtocol.Headers;

/**
 * A servlet that manages the "Query" area of the dashboard.
 * 
 * @author Andrea Gazzarini
 * @since 1.0.1
 */
public class AddOrLoadServlet extends AbstractCumulusServlet {

	private static final long serialVersionUID = -326095061705259451L;

	@Override
	public void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException, ServletException {
		request.setAttribute("page", "Load Data");
		forwardTo(request, response, "addOrLoad.vm");
	}

	@Override
	public void doPost(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		String format = request.getParameter("format");
		if (isNullOrEmptyString(format)) {
			format = "text/plain";
		}

		final String f = format;

		final HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper(request) {
			@Override
			public String getHeader(final String name) {
				return Headers.CONTENT_TYPE.equals(name) ? f : super.getParameter(name);
			}
		};

		request.setAttribute("page", "Load Data");
		request.getRequestDispatcher("/crud").forward(wrapper, response);
	}
}