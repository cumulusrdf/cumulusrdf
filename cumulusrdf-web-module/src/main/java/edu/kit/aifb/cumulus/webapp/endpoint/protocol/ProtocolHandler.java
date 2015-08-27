package edu.kit.aifb.cumulus.webapp.endpoint.protocol;

import info.aduna.webapp.views.SimpleResponseView;

import java.util.HashMap;
import java.util.Map;

import org.openrdf.http.protocol.Protocol;
import org.springframework.web.servlet.ModelAndView;

/**
 *  Interceptor for protocol requests. Should not be a singleton bean! Configure
 * as inner bean in openrdf-servlet.xml
 * @author Yongtao
 *
 */
public class ProtocolHandler {
	/**
	 * response the Protocol's version.
	 * @return the protocol's version
	 */
	public ModelAndView serve() {
		Map<String, Object> model = new HashMap<String, Object>();
		model.put(SimpleResponseView.CONTENT_KEY, Protocol.VERSION);
		return new ModelAndView(SimpleResponseView.getInstance(), model);
	}
}
