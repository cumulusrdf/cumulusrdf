package edu.kit.aifb.cumulus.integration;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.Rio;

import static edu.kit.aifb.cumulus.util.Util.parseAsList;
import static edu.kit.aifb.cumulus.util.Util.parseNX;
import static org.junit.Assert.*;

/**
 * 
 * LinkedDataServlet test without mocked objects. This currently requires a running
 * servlet container (e.g., a Tomcat server) and a cassandra server.
 * Cumulus is expected to be running at {@code http://localhost:8080/cumulusrdf-web-module/}.
 * 
 * @author Andreas Wagner
 * @since 1.1
 * @see {@link edu.kit.aifb.cumulus.webapp.LinkedDataServlet}
 * 
 */
public class LinkedDataServletClientTest extends BaseIntegrationTest {

	private static Logger _log = Logger.getLogger(LinkedDataServletClientTest.class
			.getName());

	public static void main(String[] args) throws Exception {

		LinkedDataServletClientTest test = new LinkedDataServletClientTest();

		test.post();
		test.postInvalid();

		test.get();
		test.getInvalid();

		test.delete();
		test.deleteInvalid();
	}

	protected void delete() throws IOException {

		for (int i = 0; i < URIS.size(); i++) {

			/*
			 * set-up
			 */
			String uri = URIS.get(i);
			GetMethod get = new GetMethod(uri);
			DeleteMethod del = new DeleteMethod(uri);

			assertEquals(HttpStatus.SC_OK, _client.executeMethod(get));

			/*
			 * delete triples for entity
			 */
			int status = _client.executeMethod(del);
			_log.info("delete: " + uri + ", status " + status);

			/*
			 * verify
			 */
			assertEquals(HttpStatus.SC_OK, status);
			assertEquals(HttpStatus.SC_NOT_FOUND, _client.executeMethod(get));

			/*
			 * tear-down
			 */
			del.releaseConnection();
			get.releaseConnection();
		}
	}

	protected void deleteInvalid() throws IOException {

		for (int i = 0; i < URIS_INVALID.size(); i++) {

			/*
			 * set-up
			 */
			String uri = URIS_INVALID.get(i);
			DeleteMethod del = new DeleteMethod(uri);

			/*
			 * delete triples for entity
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
				HttpMethod get = new GetMethod(uri);
				get.setRequestHeader("Accept", mime_type);

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
				//				assertEquals(mime_type,
				//						get.getResponseHeader("Content-Type").getValue());
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

			for (String mime_type : RDF_SERIALIZATIONS) {

				/*
				 * set-up
				 */
				String uri = URIS_INVALID.get(i);
				HttpMethod get = new GetMethod(uri);
				get.setRequestHeader("Accept", mime_type);

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

				PostMethod post = new PostMethod("http://localhost:8080/cumulusrdf-web-module/resource/1");
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

	protected void postInvalid() throws IOException, RDFHandlerException {

		for (int i = 0; i < TRIPLES_INVALID_NT.size(); i++) {

			/*
			 * set-up: convert to desired RDF serialization
			 */
			PostMethod post = new PostMethod("http://localhost:8080/cumulusrdf-web-module/resource/1");
			post.setRequestEntity(new StringRequestEntity(TRIPLES_INVALID_NT.get(i),
						TEXT_PLAIN, null));

			/*
			 * post data ...
			 */
			int status = _client.executeMethod(post);
			_log.info("post: content: " + TRIPLES_NT.get(i) + "\n status is:"
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