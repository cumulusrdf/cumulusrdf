package edu.kit.aifb.cumulus.webapp.endpoint.repository.size;



import info.aduna.webapp.views.SimpleResponseView;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.http.protocol.Protocol;
import org.openrdf.http.server.ProtocolUtil;
import org.openrdf.http.server.ServerHTTPException;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
//import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.springframework.web.servlet.ModelAndView;

import edu.kit.aifb.cumulus.webapp.endpoint.SesameHTTPProtocolHandler;

/**
 * Handles requests for the size of (set of contexts in) a repository.
 * 
 * The code is modified based on Sesame's org.openrdf.http.server.repository.size.SizeController
 * 
 * @author Yongtao
 *
 */
public class SizeHandler extends SesameHTTPProtocolHandler{
	@Override
	public ModelAndView serve(final Repository repository, final HttpServletRequest request, final HttpServletResponse response)
			throws Exception {
		ProtocolUtil.logRequestParameters(request);

		Map<String, Object> model = new HashMap<String, Object>();
		final boolean headersOnly = METHOD_HEAD.equals(request.getMethod());

		if (!headersOnly) {

			ValueFactory vf = repository.getValueFactory();
			Resource[] contexts = ProtocolUtil.parseContextParam(request, Protocol.CONTEXT_PARAM_NAME, vf);

			long size = -1;

			try {
				RepositoryConnection repositoryCon = repository.getConnection();
				synchronized (repositoryCon) {
					size = repositoryCon.size(contexts);
				}
				repositoryCon.close();
			} catch (RepositoryException e) {
				throw new ServerHTTPException("Repository error: " + e.getMessage(), e);
			}
			model.put(SimpleResponseView.CONTENT_KEY, String.valueOf(size));
		}

		return new ModelAndView(SimpleResponseView.getInstance(), model);
	}
}
