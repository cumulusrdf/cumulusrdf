package edu.kit.aifb.cumulus.integration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import static edu.kit.aifb.cumulus.util.Util.parseAsList;
import static org.junit.Assert.*;

/**
 * 
 * LinkedDataServlet test without mocked objects. This currently requires a running
 * servlet container (e.g., a Tomcat server) and a cassandra server.
 * Cumulus is expected to be running at {@code http://localhost:8080/cumulusrdf-web-module/}.
 * In contrast to {@link LinkedDataServletClientTest}, this test requires a mapping configuration
 * with external base URI as {@code http://example.org/}.
 * 
 * @author Andreas Wagner
 * @since 1.1
 * 
 * @see {@link edu.kit.aifb.cumulus.webapp.LinkedDataServlet}
 * @see {@link edu.kit.aifb.cumulus.integration.LinkedDataServletClientTest}
 * 
 */
public class LinkedDataServletClientTestWithMappingSail extends LinkedDataServletClientTest {

	private static Logger _log = Logger.getLogger(LinkedDataServletClientTestWithMappingSail.class
			.getName());

	protected static List<String> URIS_PUBLIC = Arrays.asList("http://example.org/resource/Actor",
				"http://example.org/resource/Thing");

	public static void main(String[] args) throws Exception {

		LinkedDataServletClientTestWithMappingSail test = new LinkedDataServletClientTestWithMappingSail();

		test.post();
		test.get();
	}

	@Override
	protected void get() throws IOException, Exception {

		for (int i = 0; i < URIS_PUBLIC.size(); i++) {

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

				for (Statement stmt : parseAsList(get.getResponseBodyAsStream(), RDFFormat.forMIMEType(mime_type))) {
					assertTrue("test failed for statement '" + stmt + "', encoded as '" + mime_type + "' ", stmt.getSubject().stringValue().equals(URIS_PUBLIC.get(i))
							|| stmt.getObject().stringValue().equals(URIS_PUBLIC.get(i)));
				}

				/*
				 * tear-down
				 */
				get.releaseConnection();
			}
		}
	}
}