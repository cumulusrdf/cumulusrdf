package edu.kit.aifb.cumulus.webapp.endpoint;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.http.protocol.Protocol;
import org.openrdf.repository.Repository;
import org.springframework.web.servlet.ModelAndView;

/**
 * An abstract class for classes that handle specific Sesame HTTP Protocol.
 * @author Yongtao
 *
 */
public abstract class SesameHTTPProtocolHandler extends Protocol {
	/** HTTP method "GET".*/
	public static final String METHOD_GET = "GET";

	/** HTTP method "HEAD". */
	public static final String METHOD_HEAD = "HEAD";

	/** HTTP method "POST". */
	public static final String METHOD_POST = "POST";

	/** Connection KEY. */
	public static final String CONNECTOIN_KEY = "sesame_repoconnecton";

	/**
	 * decode the seasme http protocol and do the corresponding work.
	 * @param repository the Repository object
	 * @param request the HttpServletRequest object
	 * @param response HttpServletRequest object
	 * @return return the MOdelAndView object which write the actual response message to the client
	 * @throws Exception the Exception happened in the serve approach
	 * @see http://openrdf.callimachus.net/sesame/2.7/docs/system.docbook?view#graph-store-support
	 */
	public abstract ModelAndView serve(Repository repository, HttpServletRequest request, HttpServletResponse response) throws Exception;
}
