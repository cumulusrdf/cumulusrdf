package edu.kit.aifb.cumulus.webapp.writer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFWriterBase;
import org.openrdf.rio.ntriples.NTriplesUtil;

/**
 * HTML RDFWriter that extends {@link org.openrdf.rio.helpers.RDFWriterBase}.
 * Prints out RDF triples (quads) in N-Triples (N-Quads) syntax.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * 
 * @see <a href="http://www.w3.org/TR/n-triples/">http://www.w3.org/TR/n-triples/</a>
 * @see <a href="http://www.w3.org/TR/n-quads/">http://www.w3.org/TR/n-quads/</a>
 * 
 * @since 1.0
 */
public class HTMLWriter extends RDFWriterBase {

	public static final RDFFormat HTML_FORMAT = new RDFFormat("HTML", Arrays.asList("text/html"), Charset.forName("UTF-8"), Arrays.asList("html", "htm"), false, false);

	private final String _title = "CumulusRDF - HTML Results";
	private Writer _writer;


	public HTMLWriter(OutputStream out) {
		this(new OutputStreamWriter(out, Charset.forName("UTF-8")));
	}

	public HTMLWriter(Writer writer) {
		_writer = writer;
	}

	@Override
	public void endRDF() throws RDFHandlerException {

		try {

			_writer.write("</body></html>");

		} catch (IOException e) {
			throw new RDFHandlerException(e);
		}
	}

	@Override
	public RDFFormat getRDFFormat() {
		return HTML_FORMAT;
	}

	@Override
	public void handleComment(String arg0) throws RDFHandlerException {

		try {

			_writer.write("<p>");
			_writer.write("# ");
			_writer.write(arg0);
			_writer.write("<p/>");

		} catch (IOException e) {
			throw new RDFHandlerException(e);
		}
	}

	@Override
	public void handleStatement(Statement arg0) throws RDFHandlerException {

		try {

			_writer.write("<p>");
			NTriplesUtil.append(arg0.getSubject(), _writer);
			_writer.write(" ");
			NTriplesUtil.append(arg0.getPredicate(), _writer);
			_writer.write(" ");
			NTriplesUtil.append(arg0.getObject(), _writer);

			if (arg0.getContext() != null) {
				_writer.write(" ");
				NTriplesUtil.append(arg0.getContext(), _writer);
			}

			_writer.write(" .");
			_writer.write("<p/>");

		} catch (IOException e) {
			throw new RDFHandlerException(e);
		}
	}

	@Override
	public void startRDF() throws RDFHandlerException {

		try {
			_writer.write("<html><head><title>" + _title + "</title></head><body>");
			_writer.write("<h1>" + _title + "</h1>");
		} catch (IOException e) {
			throw new RDFHandlerException(e);
		}
	}
}