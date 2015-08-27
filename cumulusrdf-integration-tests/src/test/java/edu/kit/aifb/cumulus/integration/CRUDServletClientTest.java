package edu.kit.aifb.cumulus.integration;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.output.ByteArrayOutputStream;

import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.Rio;

import edu.kit.aifb.cumulus.framework.Environment;
import static edu.kit.aifb.cumulus.util.Util.parseAsList;
import static edu.kit.aifb.cumulus.util.Util.parseNX;
import static org.junit.Assert.*;

/**
 * 
 * CRUDServlet test without mocked objects. This currently requires a running
 * servlet container (e.g., a Tomcat server) and a cassandra server.
 * Cumulus is expected to be running at {@code http://localhost:8080/cumulusrdf-web-module/}.
 * 
 * @author Andreas Wagner
 * @since 1.1
 * 
 * @see {@link edu.kit.aifb.cumulus.webapp.CRUDServlet}
 * @see {@link edu.kit.aifb.cumulus.webapp.CRUDServletServletDeleteTest}
 * @see {@link edu.kit.aifb.cumulus.webapp.CRUDServletServletGetTest}
 * @see {@link edu.kit.aifb.cumulus.webapp.CRUDServletServletPostTest}
 * @see {@link edu.kit.aifb.cumulus.webapp.CRUDServletServletPutTest}
 * 
 */
public class CRUDServletClientTest extends BaseIntegrationTest {

	private static final String CRUD_SERVLET = "http://localhost:8080/cumulusrdf-web-module/crud";
	private static Logger _log = Logger.getLogger(CRUDServletClientTest.class
			.getName());

	private static List<String> DELETE_PATTERN;

	static {

		DELETE_PATTERN = new LinkedList<String>();

		try {

			for (String subject : URIS) {

				DELETE_PATTERN
						.add("s="
								+ URLEncoder
										.encode("<" + subject + ">",
												"UTF-8")
								+ "&p="
								+ URLEncoder
										.encode("<" + RDF.TYPE.stringValue() + ">",
												"UTF-8")
								+ "&o="
								+ URLEncoder
										.encode("<" + RDFS.CLASS.stringValue() + ">",
												"UTF-8"));
			}

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {

		CRUDServletClientTest test = new CRUDServletClientTest();

		test.post();
		test.postInvalid();

		test.get();
		test.getInvalid();

		test.delete();
		test.deleteInvalid();
	}

	protected void delete() throws IOException {

		for (int i = 0; i < DELETE_PATTERN.size(); i++) {

			/*
			 * set-up
			 */
			DeleteMethod del = new DeleteMethod(CRUD_SERVLET);
			del.setQueryString(DELETE_PATTERN.get(i));

			/*
			 * HTTP DELETE
			 */
			int status = _client.executeMethod(del);
			_log.info("delete pattern: " + DELETE_PATTERN.get(i) + ", status " + status);

			/*
			 * verify
			 */
			assertEquals(HttpStatus.SC_OK, status);

			/*
			 * tear-down
			 */
			del.releaseConnection();
		}
	}

	protected void deleteInvalid() throws IOException {

		for (int i = 0; i < URIS_INVALID.size(); i++) {

			/*
			 * set-up
			 */
			String uri = URIS_INVALID.get(i);
			DeleteMethod del = new DeleteMethod(CRUD_SERVLET);
			del.setQueryString("uri=" + URLEncoder
										.encode("<" + uri + ">",
												"UTF-8"));

			/*
			 * HTTP DELETE
			 */
			int status = _client.executeMethod(del);
			_log.info("delete: " + uri + ", status " + status);

			/*
			 * verify
			 */
			assertEquals(HttpStatus.SC_NOT_FOUND, status);

			/*
			 * tear-down
			 */
			del.releaseConnection();
		}
	}

	protected void get() throws IOException, Exception {

		for (int i = 0; i < URIS.size(); i++) {

			for (String mime_type : RDF_SERIALIZATIONS) {

				/*
				 * set-up
				 */
				String uri = URIS.get(i);
				HttpMethod get = new GetMethod(CRUD_SERVLET);
				get.setRequestHeader("Accept", mime_type);
				get.setQueryString("uri=" + URLEncoder
						.encode("<" + uri + ">",
								"UTF-8"));

				/*
				 * get triples for entity
				 */
				int status = _client.executeMethod(get);
				_log.info("get: " + uri + ", status: " + status + ", response: "
						+ get.getResponseBodyAsString());

				/*
				 * verify
				 */
				assertEquals(HttpStatus.SC_OK, status);
				assertTrue(parseAsList(get.getResponseBodyAsStream(), RDFFormat.forMIMEType(mime_type)).size() > 0);

				/*
				 * tear-down
				 */
				get.releaseConnection();
			}
		}
	}

	protected void getInvalid() throws IOException {

		for (int i = 0; i < URIS_INVALID.size(); i++) {

			/*
			 * set-up
			 */
			String uri = URIS_INVALID.get(i);
			HttpMethod get = new GetMethod(CRUD_SERVLET);
			get.setRequestHeader("Accept", TEXT_PLAIN);
			get.setQueryString("uri=" + URLEncoder
					.encode("<" + uri + ">",
							"UTF-8"));

			/*
			 * get triples for entity
			 */
			int status = _client.executeMethod(get);
			_log.info("get: " + uri + ", status: " + status + ", response: "
					+ get.getResponseBodyAsString());

			/*
			 * verify
			 */
			assertEquals(HttpStatus.SC_NOT_FOUND, status);

			/*
			 * tear-down
			 */
			get.releaseConnection();
		}
	}

	protected void post() throws IOException, RDFHandlerException {

		for (int i = 0; i < TRIPLES_NT.size(); i++) {

			for (String mime_type : RDF_SERIALIZATIONS) {

				/*
				 * set-up: convert to desired RDF serialization
				 */
				Model model = new LinkedHashModel(parseNX(TRIPLES_NT.get(i)));
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				Rio.write(model, out, RDFFormat.forMIMEType(mime_type));

				PostMethod post = new PostMethod(CRUD_SERVLET);
				post.setRequestEntity(new ByteArrayRequestEntity(out.toByteArray(),
						mime_type));

				/*
				 * post data ...
				 */
				int status = _client.executeMethod(post);
				_log.info("post: content: " + TRIPLES_NT.get(i) + "\n status is:"
						+ status + "\n content-type: " + mime_type);

				/*
				 * verify
				 */
				assertEquals(HttpStatus.SC_CREATED, status);

				for (Statement stmt : model) {
					assertEquals(HttpStatus.SC_OK, _client.executeMethod(new GetMethod(stmt.getSubject().stringValue())));
				}

				/*
				 * tear-down
				 */
				post.releaseConnection();
			}
		}
	}

	protected void postInvalid() throws IOException {

		for (int i = 0; i < TRIPLES_INVALID_NT.size(); i++) {

			/*
			 * set-up: convert to desired RDF serialization
			 */

			PostMethod post = new PostMethod(CRUD_SERVLET);
			post.setRequestEntity(new ByteArrayRequestEntity(TRIPLES_INVALID_NT.get(i).getBytes(Environment.CHARSET_UTF8),
					TEXT_PLAIN));

			/*
			 * post data ...
			 */
			int status = _client.executeMethod(post);
			_log.info("post: content: " + TRIPLES_INVALID_NT.get(i) + "\n status is:"
					+ status + "\n content-type: " + TEXT_PLAIN);

			/*
			 * verify
			 */
			assertEquals(HttpStatus.SC_BAD_REQUEST, status);

			/*
			 * tear-down
			 */
			post.releaseConnection();
		}
	}
}