package edu.kit.aifb.cumulus.webapp.endpoint.repository.statements;

import static javax.servlet.http.HttpServletResponse.SC_OK;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.http.server.ServerHTTPException;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.repository.RepositoryConnection;
//import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.springframework.web.servlet.View;

/**
 * exports the statements to the client.
 * 
 * the class is modified based on the Seasme's org.openrdf.http.server.repository.statements.ExportStatementsView
 * 
 * @author yongtaoma
 *
 */
public final class ExportStatementsView implements View {
	public static final String SUBJECT_KEY = "subject";

	public static final String PREDICATE_KEY = "predicate";

	public static final String OBJECT_KEY = "object";

	public static final String CONTEXTS_KEY = "contexts";

	public static final String USE_INFERENCING_KEY = "useInferencing";

	public static final String FACTORY_KEY = "factory";

	public static final String HEADERS_ONLY = "headersOnly";

	public static final String REPO_KEY = "sesame_repository";

	private static final ExportStatementsView INSTANCE = new ExportStatementsView();

	/**
	 * get the instance of ExportStatementsView.
	 * @return the instance of ExportStatementsView
	 */
	public static ExportStatementsView getInstance() {
		return INSTANCE;
	}

	/**
	 * return the content type.
	 * @return null
	 */
	public String getContentType() {
		return null;
	}

	/**
	 * write the statements to the client.
	 * 
	 * @param model the Map content the request
	 * @param request the HttpServletRequest object
	 * @param response the HttpServletResponse object
	 * @throws Exception throws when errors in writing the response to the client
	 */
	@SuppressWarnings("rawtypes")
	public void render(final Map model, final HttpServletRequest request, final HttpServletResponse response)
			throws Exception {
		Resource subj = (Resource) model.get(SUBJECT_KEY);
		URI pred = (URI) model.get(PREDICATE_KEY);
		Value obj = (Value) model.get(OBJECT_KEY);
		Resource[] contexts = (Resource[]) model.get(CONTEXTS_KEY);
		boolean useInferencing = (Boolean) model.get(USE_INFERENCING_KEY);

		boolean headersOnly = (Boolean) model.get(HEADERS_ONLY);

		RDFWriterFactory rdfWriterFactory = (RDFWriterFactory) model.get(FACTORY_KEY);

		RDFFormat rdfFormat = rdfWriterFactory.getRDFFormat();

		try {
			OutputStream out = response.getOutputStream();
			RDFWriter rdfWriter = rdfWriterFactory.getWriter(out);

			response.setStatus(SC_OK);

			String mimeType = rdfFormat.getDefaultMIMEType();
			if (rdfFormat.hasCharset()) {
				Charset charset = rdfFormat.getCharset();
				mimeType += "; charset=" + charset.name();
			}
			response.setContentType(mimeType);

			String filename = "statements";
			if (rdfFormat.getDefaultFileExtension() != null) {
				filename += "." + rdfFormat.getDefaultFileExtension();
			}
			response.setHeader("Content-Disposition", "attachment; filename=" + filename);

			if (!headersOnly) {
				final SailRepository repository = (SailRepository) model.get(REPO_KEY);
				RepositoryConnection repositoryCon = repository.getConnection();
				synchronized (repositoryCon) {
					repositoryCon.exportStatements(subj, pred, obj, useInferencing, rdfWriter, contexts);
				}
			}
			out.close();
		} catch (RDFHandlerException e) {
			throw new ServerHTTPException("Serialization error: " + e.getMessage(), e);
		} catch (RepositoryException e) {
			throw new ServerHTTPException("Repository error: " + e.getMessage(), e);
		}
	}
}
