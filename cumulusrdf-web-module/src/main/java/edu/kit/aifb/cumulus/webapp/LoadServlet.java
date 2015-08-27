package edu.kit.aifb.cumulus.webapp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.openrdf.rio.RDFFormat;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.log.MessageFactory;
import edu.kit.aifb.cumulus.store.Store;

/**
 * A servlet that accepts a file containing RDF data and stores the data using
 * the cumulusrdf store.
 * 
 * @author Andreas Wagner
 */
public class LoadServlet extends AbstractCumulusServlet {

	private Log _log = new Log(LoggerFactory.getLogger(getClass()));
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {

		if (!ServletFileUpload.isMultipartContent(req)) {
			sendError(req, resp, HttpStatus.SC_BAD_REQUEST, MessageCatalog._00036_NOT_MULTIPART_REQUEST);
			return;
		}

		try {

			final File tmp_file = File.createTempFile("upload_", null);
			final FileOutputStream tmp_outstream = new FileOutputStream(tmp_file);

			List<FileItem> items = new ServletFileUpload(new DiskFileItemFactory()).parseRequest(req);

			String file_extension = null, file_name = null;
			long size_total = 0;

			for (FileItem item : items) {

				if (!item.isFormField()) {

					long size = item.getSize();
					size_total += size;

					IOUtils.copy(item.getInputStream(), tmp_outstream);

					if (file_name == null || file_extension == null) {
						file_name = FilenameUtils.getName(item.getName()).toLowerCase();
						file_extension = FilenameUtils.getExtension(item.getName()).toLowerCase();
					}

					_log.info(MessageCatalog._00037_BULK_DATA_SIZE, size);
				}
			}

			_log.info(MessageCatalog._00038_BULK_DATA_READ, size_total);
			tmp_outstream.close();

			if (file_extension == null || file_extension.isEmpty()) {
				sendError(req, resp, HttpStatus.SC_BAD_REQUEST, MessageCatalog._00039_NO_EXTENSION_FILE);
				tmp_file.delete();
				return;
			}

			RDFFormat rdf_format = RDFFormat.forFileName(file_name);

			if (rdf_format == null) {
				sendError(req, resp, HttpStatus.SC_BAD_REQUEST,
						MessageFactory.createMessage(MessageCatalog._00043_UNKNOWN_EXTENSION_FILE, file_extension));
				return;
			}
			
			Store store = (Store) getServletContext().getAttribute(ConfigParams.STORE);
			
			if ((store == null) || !store.isOpen()) {
				sendError(
						req,
						resp,
						HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
						MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG);
				return;
			}

			store.bulkLoad(tmp_file, rdf_format);
			
			tmp_file.delete();

			req.setAttribute("loadOk", true);
			req.setAttribute("page", "Load Data");
			forwardTo(req, resp, "addOrLoad.vm");

		} catch (FileUploadException exception) {
			sendError(req, resp, HttpStatus.SC_BAD_REQUEST, "cannot parse multipart request.", exception);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			sendError(req, resp, HttpServletResponse.SC_BAD_REQUEST, "System has detected and internal failure.", exception);
		}
	}
}