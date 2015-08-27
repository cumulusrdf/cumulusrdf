package edu.kit.aifb.cumulus.webapp.endpoint.repository.contexts;

import info.aduna.iteration.CloseableIteration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.http.server.ProtocolUtil;
import org.openrdf.http.server.ServerHTTPException;
import org.openrdf.http.server.repository.QueryResultView;
import org.openrdf.http.server.repository.TupleQueryResultView;
import org.openrdf.model.Resource;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.ListBindingSet;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.query.resultio.TupleQueryResultWriterFactory;
import org.openrdf.query.resultio.TupleQueryResultWriterRegistry;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
//import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.springframework.web.servlet.ModelAndView;

import edu.kit.aifb.cumulus.webapp.endpoint.SesameHTTPProtocolHandler;

/**
 * Handles requests for the list of contexts in a repository.
 * 
 * The code is modified based on Sesame's org.openrdf.http.server.repository.contexts.ContextsController
 * 
 * @author Yongtao
 *
 */
public class ContextsHandler extends SesameHTTPProtocolHandler {
	@Override
	public ModelAndView serve(final Repository repository, final HttpServletRequest request, final HttpServletResponse response)
			throws Exception {
		Map<String, Object> model = new HashMap<String, Object>();
		TupleQueryResultWriterFactory factory = ProtocolUtil.getAcceptableService(request, response,
				TupleQueryResultWriterRegistry.getInstance());

		if (METHOD_GET.equals(request.getMethod())) {
			List<String> columnNames = Arrays.asList("contextID");
			List<BindingSet> contexts = new ArrayList<BindingSet>();
			RepositoryConnection repositoryCon = repository.getConnection();
			synchronized (repositoryCon) {
				try {
					CloseableIteration<? extends Resource, RepositoryException> contextIter = repositoryCon.getContextIDs();

					try {
						while (contextIter.hasNext()) {
							BindingSet bindingSet = new ListBindingSet(columnNames, contextIter.next());
							contexts.add(bindingSet);
						}
					} finally {
						contextIter.close();
					}
				} catch (RepositoryException e) {
					throw new ServerHTTPException("Repository error: " + e.getMessage(), e);
				}
			}
			model.put(QueryResultView.QUERY_RESULT_KEY, new TupleQueryResultImpl(columnNames, contexts));
		}

		model.put(QueryResultView.FILENAME_HINT_KEY, "contexts");
		model.put(QueryResultView.FACTORY_KEY, factory);
		model.put(QueryResultView.HEADERS_ONLY, METHOD_HEAD.equals(request.getMethod()));
		return new ModelAndView(TupleQueryResultView.getInstance(), model);
	}
}
