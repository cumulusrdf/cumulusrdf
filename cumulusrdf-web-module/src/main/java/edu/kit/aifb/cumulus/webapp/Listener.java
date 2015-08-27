package edu.kit.aifb.cumulus.webapp;

import static edu.kit.aifb.cumulus.webapp.writer.HTMLWriter.HTML_FORMAT;

import java.io.OutputStream;
import java.io.Writer;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import net.fortytwo.sesametools.mappingsail.MappingSail;
import net.fortytwo.sesametools.mappingsail.MappingSchema;
import net.fortytwo.sesametools.mappingsail.RewriteRule;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.sail.Sail;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.Environment.ConfigParams;
import edu.kit.aifb.cumulus.framework.Environment.ConfigValues;
import edu.kit.aifb.cumulus.framework.domain.configuration.Configurable;
import edu.kit.aifb.cumulus.framework.domain.configuration.Configuration;
import edu.kit.aifb.cumulus.framework.domain.configuration.DefaultConfigurator;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.QuadStore;
import edu.kit.aifb.cumulus.store.RepositoryManager;
import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.TripleStore;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFSail;
import edu.kit.aifb.cumulus.webapp.writer.HTMLWriter;

/**
 * CumulusRDF (Web) Application lifecycle listener.
 * 
 * @author aharth
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Listener implements ServletContextListener, Configurable<Map<String, Object>> {

	/*
	 * register additional Sesame RDF writers/parsers
	 */
	static {

		RDFFormat.register(HTML_FORMAT);
		RDFWriterRegistry.getInstance().add(new RDFWriterFactory() {

			@Override
			public RDFFormat getRDFFormat() {
				return HTML_FORMAT;
			}

			@Override
			public RDFWriter getWriter(OutputStream out) {
				return new HTMLWriter(out);
			}

			@Override
			public RDFWriter getWriter(Writer writer) {
				return new HTMLWriter(writer);
			}
		});
	}

	private final Log _log = new Log(LoggerFactory.getLogger(Listener.class));

	private ServletContext _applicationContext;

	@Override
	public void contextDestroyed(final ServletContextEvent event) {
		_log.info(MessageCatalog._00016_CDRF_SHUTDOWN_START);

		RepositoryManager.getInstance().shutDownAll();
		_applicationContext = null;

		_log.info(MessageCatalog._00017_CRDF_SHUTDOWN_END);
	}

	@Override
	public void contextInitialized(final ServletContextEvent event) {
		_log.info(MessageCatalog._00001_CRDF_STARTUP);

		_applicationContext = event.getServletContext();

		System.err.close();

		final Configuration<Map<String, Object>> configuration = new DefaultConfigurator();
		configuration.configure(this);
	}

	@Override
	public void accept(final Configuration<Map<String, Object>> configuration) {

		_log.info(MessageCatalog._00009_REPOSITORY_INITIALIZED);
		final Set<String> identifiers = configuration.getDeclaredIdentifiers();

		/* 
		 * Case #1: configuration contains a single store.
		 */
		if (identifiers == null || identifiers.isEmpty()) {

			final String layout = configuration.getAttribute(ConfigParams.LAYOUT, ConfigValues.STORE_LAYOUT_TRIPLE);
			final Store store = (ConfigValues.STORE_LAYOUT_QUAD.equals(layout)) ? new QuadStore() : new TripleStore();
			final String internalBaseURI = configuration.getAttribute(ConfigParams.INTERNAL_BASE_URI, null);
			final String externalBaseURI = configuration.getAttribute(ConfigParams.EXTERNAL_BASE_URI, null);

			registerNewRepository(store.getId(), store, true, internalBaseURI, externalBaseURI);

			_applicationContext.setAttribute(ConfigParams.STORE, store);
			_applicationContext.setAttribute(ConfigParams.LAYOUT, layout);

		} else {

			/* 
			 * Case #2: configuration contains multiple stores
			 */
			final String defaultStoreId = configuration.getAttribute(ConfigParams.DEFAULT_STORE, (String) null);

			for (final String id : identifiers) {

				final String layout = configuration.getAttribute(id + "." + ConfigParams.LAYOUT, ConfigValues.STORE_LAYOUT_TRIPLE);
				final Store store = (ConfigValues.STORE_LAYOUT_QUAD.equals(layout)) ? new QuadStore(id) : new TripleStore(id);
				final boolean isDefault = id.equals(defaultStoreId);
				final String internalBaseURI = configuration.getAttribute(ConfigParams.INTERNAL_BASE_URI, null);
				final String externalBaseURI = configuration.getAttribute(ConfigParams.EXTERNAL_BASE_URI, null);

				registerNewRepository(store.getId(), store, isDefault, internalBaseURI, externalBaseURI);

				_applicationContext.setAttribute(id + "." + ConfigParams.STORE, store);
				_applicationContext.setAttribute(id + "." + ConfigParams.LAYOUT, layout);
			}
		}
	}

	/**
	 * Registers a new repository.
	 * 
	 * @param id - id the repository (store) identifier.
	 * @param store - the store.
	 * @param internalBaseURI - base URI of resources within the data store
	 * @param externalBaseURI - base URI of resources as they are to be seen on the Web
	 * @param isDefault - indicates if it must be considered the default target for incoming requests that doesn't specify an id.
	 */
	void registerNewRepository(final String id, final Store store, final boolean isDefault, final String internalBaseURI, final String externalBaseURI) {

		Repository repository = null;

		try {

			Sail sail = new CumulusRDFSail(store);

			/*
			 * if internal/external base URI is given, we add a MappingSail on top of the CumulusRDFSail
			 */
			if (internalBaseURI != null && !internalBaseURI.isEmpty() && externalBaseURI != null && !externalBaseURI.isEmpty() &&
					!internalBaseURI.equals(externalBaseURI)) {

				RewriteRule rewriter_out = new RewriteRule() {
					public URI rewrite(final URI original) {

						if (null == original) {
							return null;
						} else {
							String s = original.stringValue();
							return s.startsWith(internalBaseURI)
										? new URIImpl(s.replace(internalBaseURI, externalBaseURI))
										: original;
						}
					}
				};

				RewriteRule rewriter_in = new RewriteRule() {
					public URI rewrite(final URI original) {

						if (null == original) {
							return null;
						} else {
							String s = original.stringValue();
							return s.startsWith(externalBaseURI)
										? new URIImpl(s.replace(externalBaseURI, internalBaseURI))
										: original;
						}
					}
				};

				MappingSchema schema = new MappingSchema();
				schema.setRewriter(MappingSchema.Direction.INBOUND, rewriter_in);
				schema.setRewriter(MappingSchema.Direction.OUTBOUND, rewriter_out);
				sail = new MappingSail(sail, schema);
			}

			repository = new SailRepository(sail);
			repository.initialize();

			RepositoryManager.getInstance().addRepository(id, repository, isDefault);

			if (isDefault) {
				_applicationContext.setAttribute(ConfigParams.SESAME_REPO, repository);
			}

			_log.info(MessageCatalog._00010_CRDF_STARTED);
		} catch (final Exception exception) {

			_log.error(MessageCatalog._00011_REPOSITORY_INITIALISATION_FAILURE, exception);
			_applicationContext.setAttribute(ConfigParams.ERROR, exception);
			shutdown(repository);

			_log.warning(MessageCatalog._00015_CRDF_STARTED_WITH_FAILURES);
		}
	}

	/**
	 * Shutdown a repository.
	 * Tries to shutdown the underlying store too.
	 * 
	 * @param repository the repository.
	 * @param store the store.
	 */
	void shutdown(final Repository repository) {
		if (repository != null) {
			_log.info(MessageCatalog._00012_REPOSITORY_SHUTDOWN_START);

			try {
				repository.shutDown();
				_log.info(MessageCatalog._00014_REPOSITORY_SHUTDOWN);
			} catch (final RepositoryException repositoryException) {
				_log.error(MessageCatalog._00013_REPOSITORY_SHUTDOWN_FAILURE, repositoryException);
			}
		}
	}
}