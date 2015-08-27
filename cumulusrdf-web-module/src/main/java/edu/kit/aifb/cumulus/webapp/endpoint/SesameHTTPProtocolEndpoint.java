package edu.kit.aifb.cumulus.webapp.endpoint;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.http.protocol.Protocol;
import org.openrdf.repository.sail.SailRepository;
import org.springframework.web.servlet.ModelAndView;

import edu.kit.aifb.cumulus.store.RepositoryManager;
import edu.kit.aifb.cumulus.webapp.AbstractCumulusServlet;
import edu.kit.aifb.cumulus.webapp.endpoint.protocol.ProtocolHandler;
import edu.kit.aifb.cumulus.webapp.endpoint.repository.RepositoryHandler;
import edu.kit.aifb.cumulus.webapp.endpoint.repository.contexts.ContextsHandler;
import edu.kit.aifb.cumulus.webapp.endpoint.repository.graph.GraphHandler;
import edu.kit.aifb.cumulus.webapp.endpoint.repository.namespaces.NamespaceHandler;
import edu.kit.aifb.cumulus.webapp.endpoint.repository.namespaces.NamespacesHandler;
import edu.kit.aifb.cumulus.webapp.endpoint.repository.size.SizeHandler;
import edu.kit.aifb.cumulus.webapp.endpoint.repository.statements.StatementHandler;

/**
 * SesameHTTPRespositoryAdapter is a servlet, which dispatches incoming Sesame HTTP Protocol requests.
 * 
 * @author Yongtao Ma
 * @see <a href="org.openrdf.http.client.HTTPClient">org.openrdf.http.client.HTTPClient</a> 
 * @see <a href="http://openrdf.callimachus.net/sesame/2.7/docs/system.docbook?view#graph-store-support">http://openrdf.callimachus.net/sesame/2.7/docs/system.docbook?view#graph-store-support</a> 
 */
public class SesameHTTPProtocolEndpoint extends AbstractCumulusServlet {

	private static final long serialVersionUID = 1L;

	@Override
	public void service(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		String requestUri = request.getRequestURI();


		ModelAndView result = null;
		//the requestURI must refer to the servelet URI that begins with "/protocol" or "/repositories"
		try {
			//if the requestUri end with "/protocol"
			//it requests the protocol version (GET)
			if (requestUri.endsWith("/" + Protocol.PROTOCOL)) {
				result = (new ProtocolHandler()).serve();
			} else {
				String id = requestUri.substring(requestUri.indexOf("repositories/") + "repositories/".length());
				if (id.contains("/")) {
					id = id.substring(0, id.indexOf("/"));
				}
				final SailRepository repository = (SailRepository) RepositoryManager.getInstance().getRepository(id);
				if (repository == null) {
					response.setStatus(HttpServletResponse.SC_NOT_FOUND);
					return;
				}
				if (requestUri.endsWith("/" + Protocol.STATEMENTS)) {
					//if the requestUsri ends with "/statements"
					//it requests on repository statements (GET/POST/PUT/DELETE)
					result = (new StatementHandler()).serve(repository, request, response);
				} else if (requestUri.endsWith("/" + Protocol.CONTEXTS)) {
					//if the requestUri ends with "/contexts"
					//it requests context overview (GET)
					result = (new ContextsHandler()).serve(repository, request, response);
				} else if (requestUri.endsWith("/" + Protocol.SIZE)) {
					//if the requestUri ends with "/size"
					//it requests # statements in repository (GET)
					result = (new SizeHandler()).serve(repository, request, response);
				} else if (requestUri.matches("(/|^)(\\w|\\-|\\_)*/repositories/(\\w|\\-|\\_)(\\w|\\-|\\_)*/rdf-graphs(/(\\w|\\-|\\_)(\\w|\\-|\\_)*|$)")) {
					//if the requestUri ends with "/rdf-graphs/*"
					//it requests according to the follows:
					//"/rdf-graphs": named graphs overview (GET)
					//"/ref-graphs/service": SPARQL Graph Store operations on indirectly referenced named graphs in repository (GET/PUT/POST/DELETE)
					//"/ref-graphs/<NAME>": SPARQL Graph Store operationson directly referenced named graphs in repository (GET/PUT/POST/DELETE)
					result = (new GraphHandler()).serve(repository, request, response);
				} else if (requestUri.endsWith("/" + Protocol.NAMESPACES)) {
					//if the requestUri ends with "/namespaces"
					//it requests overview of namespace definitions (GET/DELETE)
					result = (new NamespacesHandler()).serve(repository, request, response);
				} else if (requestUri.matches("(/|^)(\\w|\\-|\\_)*/repositories/(\\w|\\-|\\_)(\\w|\\-|\\_)*/namespaces/(\\w|\\-|\\_)(\\w|\\-|\\_)*")) {
					//if the request matches with "/namespaces/<PREFIX>"
					//it requests namespace-prefix definition (GET/PUT/DELETE)
					result = (new NamespaceHandler()).serve(repository, request, response);
				} else if (requestUri.matches("(/|^)(\\w|\\-|\\_)*/repositories/\\w(\\w|\\-|\\_)*$")) {
					//else it requests the repository information
					result = (new RepositoryHandler()).serve(repository, request, response);
				} else if (requestUri.endsWith("/repositories")) {
					result = (new RepositoryHandler()).serve(repository, request, response);
				}
			}
			result.getView().render(result.getModel(), request, response);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
