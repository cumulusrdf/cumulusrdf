package edu.kit.aifb.cumulus.webapp;

import static edu.kit.aifb.cumulus.framework.util.Strings.isNotNullOrEmptyString;
import static edu.kit.aifb.cumulus.framework.util.Strings.isNullOrEmptyString;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import javax.servlet.http.HttpServletRequest;

import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.rio.RDFFormat;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.util.Strings;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.util.Util;

/**
 * Provides various HTTP utilities and attributes used in 
 * CumulusRDF HTTP servlets.
 * 
 * @author Andrea Gazzarini
 * @author Andreas Wagner
 * @since 1.0.1
 */
public class HttpProtocol {

	/**
	 * 
	 * HTTP header fields, which are used in CumulusRDF servlets.
	 * 
	 * @author Andreas Wagner
	 * @since 1.1
	 * @see <a href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html</a>
	 *
	 */
	public interface Headers {
		static final String CONTENT_TYPE = Parameters.CONTENT_TYPE, ACCEPT = Parameters.ACCEPT, BASE_URI = Parameters.BASE_URI;
	}

	/**
	 * 
	 * HTTP methods that are used in the CumulusRDF servlets.
	 * 
	 * @author Andreas Wagner
	 * @since 1.1
	 *
	 */
	public interface Methods {
		static final String PUT = "PUT", POST = "POST", DELETE = "DELETE", GET = "GET";
	}

	/**
	 * MIME types for RDF serialization as well as result serialization.
	 * 
	 * @author Andreas Wagner
	 * @since 1.1
	 * @see <a href="http://www.iana.org/assignments/media-types/media-types.xhtml">http://www.iana.org/assignments/media-types/media-types.xhtml</a>
	 *
	 */
	public interface MimeTypes {

		/*
		 * MIME types for RDF serializations
		 */
		static final String RDF_XML = "application/rdf+xml",
				BINARY = "application/x-binary-rdf",
				N_TRIPLES = "text/plain", //application/n-triples
				N_QUADS = "text/x-nquads", //application/n-quads
				JSON_LD = "application/ld+json",
				RDF_JSON = "application/rdf+json",
				TRIG = "application/x-trig",
				TRIX = "application/trix",
				TURTLE = "text/turtle",
				TURTLE_ALT = "application/x-turtle",
				N3 = "text/n3",
				N3_ALT = "text/rdf+n3",
				TEXT_PLAIN = "text/plain",
				TEXT_HTML = "text/html";

		static final String[] RDF_SERIALIZATIONS = new String[] { RDF_XML, BINARY, N_TRIPLES, N_QUADS, JSON_LD, RDF_JSON, TRIG, TRIX, TURTLE, TURTLE_ALT, N3, N3_ALT, TEXT_PLAIN };

		/*
		 * MIME types for result serializations
		 */
		static final String
				SPARQL_XML = "application/sparql-results+xml",
				SPARQL_BINARY = "application/x-binary-rdf-results-table",
				SPARQL_JSON = "application/sparql-results+json",
				SPARQL_CSV = "text/csv",
				SPARQL_TSV = "text/tab-separated-values",
				SPARQL_BOOLEAN = "text/boolean";

		static final String[] RESULT_SERIALIZATIONS = new String[] { SPARQL_XML, SPARQL_BINARY, SPARQL_JSON, SPARQL_CSV, SPARQL_TSV, SPARQL_BOOLEAN };
	}

	/**
	 * Parameters used in CumulusRDF servlets.
	 * 
	 * @author Andreas Wagner
	 * @since 1.1
	 */
	public interface Parameters {

		static final String URI = "uri",
				S = "s",
				P = "p",
				O = "o",
				C = "c",
				S2 = "s2",
				P2 = "p2",
				O2 = "o2",
				C2 = "c2",
				ACCEPT = "accept",
				QUERY = "query",
				UPDATE = "update",
				BASE_URI = "base-uri",
				CONTENT_TYPE = "content-type";
	}

	private static final String DEFAULT_URL_ENCODING = "UTF-8";
	private static final Log _log = new Log(LoggerFactory.getLogger(HttpProtocol.class));

	/**
	 * 
	 * Retrieves and decodes (if HTTP request uses GET or DELETE method) a parameter value from the HTTP request.
	 * 
	 * @author Andreas Wagner
	 * @since 1.1
	 * 
	 * @param request - HTTP request
	 * @param parameter - parameter name
	 * @return parameter value
	 */
	public static String getParameterValue(final HttpServletRequest request, String parameter) {

		boolean url_encoded = request.getMethod().equals(Methods.GET) || request.getMethod().equals(Methods.DELETE) ? true : false;
		String value = request.getParameter(parameter), encoding = request.getCharacterEncoding() == null ? DEFAULT_URL_ENCODING : request.getCharacterEncoding();

		try {
			return (value == null) || value.isEmpty() || !url_encoded ? value : URLDecoder.decode(value, encoding);
		} catch (UnsupportedEncodingException e) {
			_log.warning(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG + " Could not decode HTTP parameter-value '" + value + "' for paramter '" + parameter + "'", e);
			return null;
		}
	}

	/**
	 * <p>Retrieves the MIME type for the accept parameter.
	 * The accept parameter could be specified by the client as a parameter or 
	 * a header in the HTTP request.
	 * HTTP header has a higher priority.</p>
	 * <p> Default (if no accept parameter is given): RDF/XML.</p>
	 * 
	 * @author Andreas Wagner
	 * @since 1.1
	 * 
	 * @param request - HTTP request.
	 * @return MIME type of the accept parameter.
	 */
	public static String parseAcceptHeader(final HttpServletRequest request) {

		String accept = request.getHeader(Headers.ACCEPT);

		if (isNullOrEmptyString(accept)) {
			accept = getParameterValue(request, Parameters.ACCEPT);
		}

		if (isNotNullOrEmptyString(accept)) {
			accept = accept.trim().toLowerCase();
		}

		if ((BooleanQueryResultFormat.forMIMEType(accept) == null) &&
				(TupleQueryResultFormat.forMIMEType(accept) == null) &&
				(RDFFormat.forMIMEType(accept) == null)) {

			/*
			 * guess MIME type
			 */

			/*
			 * default: MimeTypes.RDF_XML
			 */
			if ((accept == null) || accept.isEmpty()) {
				accept = MimeTypes.RDF_XML;
			}
			/*
			 * MIME type for result serialization
			 */
			else if (accept.contains("sparql") || accept.contains("result") || accept.contains("tsv") || accept.contains("tab") || accept.contains("csv")
					|| accept.contains("bool")) {

				if (accept.contains("xml")) {
					accept = MimeTypes.SPARQL_XML;
				} else if (accept.contains("binary")) {
					accept = MimeTypes.SPARQL_BINARY;
				} else if (accept.contains("json")) {
					accept = MimeTypes.SPARQL_JSON;
				} else if (accept.contains("csv")) {
					accept = MimeTypes.SPARQL_CSV;
				} else if (accept.contains("tsv") || accept.contains("tab")) {
					accept = MimeTypes.SPARQL_TSV;
				} else if (accept.contains("bool")) {
					accept = MimeTypes.SPARQL_BOOLEAN;
				}
				/*
				 * default: SPARQL/XML				
				 */
				else {
					accept = MimeTypes.SPARQL_XML;
				}
			}
			/*
			 * MIME type for RDF serialization
			 */
			else {

				if (accept.contains("htm")) {
					accept = MimeTypes.TEXT_HTML;
				} else if (accept.contains("xml")) {
					accept = MimeTypes.RDF_XML;
				} else if (accept.contains("rdf+json")) {
					accept = MimeTypes.RDF_JSON;
				} else if (accept.contains("json")) {
					accept = MimeTypes.JSON_LD;
				} else if (accept.contains("trix")) {
					accept = MimeTypes.TRIX;
				} else if (accept.contains("trig")) {
					accept = MimeTypes.TRIG;
				} else if (accept.contains("n3")) {
					accept = MimeTypes.N3;
				} else if (accept.contains("turtle") || accept.contains("ttl")) {
					accept = MimeTypes.TURTLE;
				} else if (accept.contains("triple")) {
					accept = MimeTypes.TEXT_PLAIN;
				} else if (accept.contains("quad")) {
					accept = MimeTypes.N_QUADS;
				} else if (accept.contains("plain")) {
					accept = MimeTypes.TEXT_PLAIN;
				} else if (accept.contains("binary")) {
					accept = MimeTypes.BINARY;
				}
				/*
				 * default: RDF/XML				
				 */
				else {
					accept = MimeTypes.RDF_XML;
				}
			}
		}

		return accept;
	}

	/**
	 * Parses the base URI from the header or as request parameter.
	 * 
	 * @author Andreas Wagner
	 * @since 1.1
	 * 
	 * @param request - HTTP request 
	 * @return base URI
	 *  
	 */
	public static String parseBaseURI(final HttpServletRequest request) {
		return parseBaseURI(request, Strings.EMPTY_STRING);
	}

	/**
	 * Parses the base URI from the header or as request parameter.
	 * 
	 * @author Andreas Wagner
	 * @since 1.1
	 * 
	 * @param request - HTTP request
	 * @param default_base_URI - default value
	 * @return base URI
	 * 
	 */
	public static String parseBaseURI(final HttpServletRequest request, final String default_base_URI) {

		String base_URI = getParameterValue(request, Parameters.BASE_URI);

		if (isNullOrEmptyString(base_URI)) {
			base_URI = request.getHeader(Headers.BASE_URI);
		}

		if (isNotNullOrEmptyString(base_URI)) {
			return Util.isValidURI(base_URI) ? base_URI : default_base_URI;
		} else {
			return default_base_URI;
		}
	}

	/**
	 * <p>Retrieves the MIME type for the content-type parameter.
	 * The content-type parameter could be specified by the client as a parameter or 
	 * a header in the HTTP request.
	 * HTTP header has a higher priority.</p>
	 * <p> Default (if no content-type parameter is given): RDF/XML.</p>
	 * 
	 * @param request - the HTTP request.
	 * @return MIME type of the content-type parameter.
	 */
	public static String parseContentTypeHeader(final HttpServletRequest request) {

		String content_type = request.getHeader(Headers.CONTENT_TYPE);

		if (isNullOrEmptyString(content_type)) {
			content_type = getParameterValue(request, Parameters.CONTENT_TYPE);
		}

		if (isNotNullOrEmptyString(content_type)) {
			content_type = content_type.trim().toLowerCase();
		}

		if (RDFFormat.forMIMEType(content_type) == null) {

			/*
			 * default: MimeTypes.RDF_XML
			 */
			if ((content_type == null) || content_type.isEmpty()) {
				content_type = MimeTypes.RDF_XML;
			}
			/*
			 * guess MIME type
			 */
			else if (content_type.contains("htm")) {
				content_type = MimeTypes.TEXT_HTML;
			} else if (content_type.contains("xml")) {
				content_type = MimeTypes.RDF_XML;
			} else if (content_type.contains("rdf+json")) {
				content_type = MimeTypes.RDF_JSON;
			} else if (content_type.contains("json")) {
				content_type = MimeTypes.JSON_LD;
			} else if (content_type.contains("trix")) {
				content_type = MimeTypes.TRIX;
			} else if (content_type.contains("trig")) {
				content_type = MimeTypes.TRIG;
			} else if (content_type.contains("n3")) {
				content_type = MimeTypes.N3;
			} else if (content_type.contains("turtle") || content_type.contains("ttl")) {
				content_type = MimeTypes.TURTLE;
			} else if (content_type.contains("triple")) {
				content_type = MimeTypes.TEXT_PLAIN;
			} else if (content_type.contains("quad")) {
				content_type = MimeTypes.N_QUADS;
			} else if (content_type.contains("plain")) {
				content_type = MimeTypes.TEXT_PLAIN;
			} else if (content_type.contains("binary")) {
				content_type = MimeTypes.BINARY;
			}
			/*
			 * default: RDF/XML				
			 */
			else {
				content_type = MimeTypes.RDF_XML;
			}
		}

		return content_type;
	}

	private HttpProtocol() {

	}
}