package edu.kit.aifb.cumulus.integration;

//import java.io.IOException;
//import java.io.UnsupportedEncodingException;
//import java.net.URLEncoder;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.logging.Logger;
//
//import org.apache.commons.httpclient.HttpMethod;
//import org.apache.commons.httpclient.HttpStatus;
//import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
//import org.apache.commons.httpclient.methods.DeleteMethod;
//import org.apache.commons.httpclient.methods.GetMethod;
//import org.apache.commons.httpclient.methods.PostMethod;
//import org.apache.commons.io.output.ByteArrayOutputStream;
//
//import org.openrdf.model.Model;
//import org.openrdf.model.Statement;
//import org.openrdf.model.impl.LinkedHashModel;
//import org.openrdf.model.vocabulary.RDF;
//import org.openrdf.model.vocabulary.RDFS;
//import org.openrdf.rio.RDFFormat;
//import org.openrdf.rio.RDFHandlerException;
//import org.openrdf.rio.Rio;
//
//import edu.kit.aifb.cumulus.framework.Environment;
//import static edu.kit.aifb.cumulus.util.Util.parseAsList;
//import static edu.kit.aifb.cumulus.util.Util.parseNX;
//import static org.junit.Assert.*;

/**
 * 
 * SPARQLServlet test without mocked objects. This currently requires a running
 * servlet container (e.g., a Tomcat server) and a cassandra server.
 * Cumulus is expected to be running at {@code http://localhost:8080/cumulusrdf-web-module/}.
 * 
 * @author Andreas Wagner
 * @since 1.1
 * 
 * @see {@link edu.kit.aifb.cumulus.webapp.SPARQLServlet}
 * @see {@link edu.kit.aifb.cumulus.webapp.SPARQLServletTest}
 * @see {@link edu.kit.aifb.cumulus.webapp.SPARQLServletTest2}
 * 
 */
public class SPARQLServletClientTest extends BaseIntegrationTest {
// TODO
//	private static final String SPARQL_SERVLET = "http://localhost:8080/cumulusrdf-web-module-1.1.0-SNAPSHOT/sparql";
//	private static Logger _log = Logger.getLogger(SPARQLServletClientTest.class
//			.getName());
//
//
//	public static void main(String[] args) throws Exception {
//
//		SPARQLServletClientTest test = new SPARQLServletClientTest();
//
//		test.post();
//		test.query();
//	}
//
//	protected void post() throws IOException, RDFHandlerException {
//		
//		for (int i = 0; i < TRIPLES_NT.size(); i++) {
//
//			for (String mime_type : RDF_SERIALIZATIONS) {
//
//				/*
//				 * set-up: convert to desired RDF serialization
//				 */
//				Model model = new LinkedHashModel(parseNX(TRIPLES_NT.get(i)));
//				ByteArrayOutputStream out = new ByteArrayOutputStream();
//				Rio.write(model, out, RDFFormat.forMIMEType(mime_type));
//
//				PostMethod post = new PostMethod(CRUD_SERVLET);
//				post.setRequestEntity(new ByteArrayRequestEntity(out.toByteArray(),
//						mime_type));
//
//				/*
//				 * post data ...
//				 */
//				int status = _client.executeMethod(post);
//				_log.info("post: content: " + TRIPLES_NT.get(i) + "\n status is:"
//						+ status + "\n content-type: " + mime_type);
//
//				/*
//				 * verify
//				 */
//				assertEquals(HttpStatus.SC_CREATED, status);
//
//				for (Statement stmt : model) {
//					assertEquals(HttpStatus.SC_OK, _client.executeMethod(new GetMethod(stmt.getSubject().stringValue())));
//				}
//
//				/*
//				 * tear-down
//				 */
//				post.releaseConnection();
//			}
//		}
//	}

}