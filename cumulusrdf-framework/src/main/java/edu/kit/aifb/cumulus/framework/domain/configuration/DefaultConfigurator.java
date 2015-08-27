package edu.kit.aifb.cumulus.framework.domain.configuration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * CumulusRDF default configurator.
 * Implements a finite state machine trying to find the configuration according with the following chain:
 * 
 * <ol>
 * 	<li>If a <b>crdf.config.file</b> system property is found, then that file will be loaded.</li>
 * 	<li>If a <b>crdf.config.dir</b> system property is found, then a file in that directory called <i>cumulusRDF.yaml</i> will be loaded.</li>
 * 	<li>The system will try to load a classpath resource called /cumulusRDF.yaml</li>
 * 	<li>The system will use the embedded default classpath resource cumulusRDF-default.yaml</li> 
 * </ol>
 * 
 * Whenever the precondition of each step fails or a failure is met, then the system will try the next step.
 * 
 * @see http://en.wikipedia.org/wiki/Finite-state_machine
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class DefaultConfigurator implements Configuration<Map<String, Object>> {
	
	public static final String CONFIG_FILE_SYSTEM_PROPERTY_NAME = "crdf.config.file";
	public static final String CONFIG_DIR_SYSTEM_PROPERTY_NAME = "crdf.config.dir";
	public static final String CONFIG_FILE_NAME = "cumulusRDF.yaml";
	public static final String DEFAULT_CONFIG_FILE_NAME = "cumulusRDF-default.yaml";

	protected final Log _log = new Log(LoggerFactory.getLogger(DefaultConfigurator.class));
	protected final String _storeId;
	
	/**
	 * Supertype layer for configurator states.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.1.0
	 */
	interface ConfigurationState {

		/**
		 * Defines a specific behavior in case the configurator will be in this state.
		 * 
		 * @param configurable the configurable target.
		 */
		void configure(final Configurable configurable);
	};

	/**
	 * Tries to find a given configuration file.
	 */
	final ConfigurationState _tryWithConfigurationFile = new ConfigurationState() {

		@Override
		public void configure(final Configurable configurable) {
			final String configFilePath = System.getProperty(CONFIG_FILE_SYSTEM_PROPERTY_NAME);
			try {
				if (configFilePath != null) {
					final File configFile = new File(configFilePath);
					if (configFile.canRead()) {
						final Yaml loader = new Yaml();
						_attributes = (Map<String, Object>) loader.load(new FileReader(configFile));

						_log.info(MessageCatalog._00101_USING_CONFIG_FILE, configFile);
						configurable.accept(DefaultConfigurator.this);
					} else {
						_log.error(MessageCatalog._00100_CONFIG_FILE_NOT_READABLE, configFilePath);
						transitionTo(_tryWithConfigurationDirectory, configurable);
					}
				} else {
					transitionTo(_tryWithConfigurationDirectory, configurable);
				}
			} catch (final FileNotFoundException exception) {
				_log.error(MessageCatalog._00099_CONFIG_FILE_NOT_FOUND, configFilePath);
				transitionTo(_tryWithConfigurationDirectory, configurable);
			}
		}
	};

	/**
	 * Tries to find the configuration file under a given directory.
	 */
	final ConfigurationState _tryWithConfigurationDirectory = new ConfigurationState() {

		@Override
		public void configure(final Configurable configurable) {
			final String configDirPath = System.getProperty(CONFIG_DIR_SYSTEM_PROPERTY_NAME);
			if (configDirPath != null) {
				final File configFile = new File(configDirPath, CONFIG_FILE_NAME);
				try {
					if (configFile.canRead()) {
						final Yaml loader = new Yaml();
						_attributes = (Map<String, Object>) loader.load(new FileReader(configFile));

						_log.info(MessageCatalog._00101_USING_CONFIG_FILE, configFile);
						configurable.accept(DefaultConfigurator.this);
					} else {
						_log.error(MessageCatalog._00100_CONFIG_FILE_NOT_READABLE, configFile);
						transitionTo(_tryWithClasspathResource, configurable);
					}
				} catch (final FileNotFoundException exception) {
					_log.error(MessageCatalog._00099_CONFIG_FILE_NOT_FOUND, configFile);
					transitionTo(_tryWithClasspathResource, configurable);
				}
			} else {
				transitionTo(_tryWithClasspathResource, configurable);
			}
		}
	};
	
	/**
	 * Tries with a classpath resource.
	 */
	final ConfigurationState _tryWithClasspathResource = new ConfigurationState() {
		@Override
		public void configure(final Configurable configurable) {
			final InputStream stream = getClass().getResourceAsStream("/" + CONFIG_FILE_NAME);
			if (stream != null) {
				final Yaml loader = new Yaml();
				_attributes = (Map<String, Object>) loader.load(stream);

				_log.info(MessageCatalog._00102_USING_CLASSPATH_RESOURCE, CONFIG_FILE_NAME);
				configurable.accept(DefaultConfigurator.this);
			} else {
				transitionTo(_useEmbeddedConfiguration, configurable);
			}
		}
	};
	
	/**
	 * Uses the default embedded configuration.
	 */
	final ConfigurationState _useEmbeddedConfiguration = new ConfigurationState() {

		@Override
		public void configure(final Configurable configurable) {
			final Yaml loader = new Yaml();
			_attributes = (Map<String, Object>) loader.load(getClass().getResourceAsStream("/" + DEFAULT_CONFIG_FILE_NAME));

			_log.info(MessageCatalog._00103_USING_EMBEDDED_CONFIGURATION, DEFAULT_CONFIG_FILE_NAME);

			configurable.accept(DefaultConfigurator.this);
			_currentState = _configurationHasBeenLoaded;
		}
	};

	final ConfigurationState _configurationHasBeenLoaded = new ConfigurationState() {

		@Override
		public void configure(final Configurable configurable) {
			configurable.accept(DefaultConfigurator.this);
		}
	};

	protected Map<String, Object> _attributes;
	protected ConfigurationState _currentState = _tryWithConfigurationFile;
	
	/**
	 * Builds an anonymous configurator.
	 */
	public DefaultConfigurator() {
		_storeId = null;
	}

	/**
	 * Builds an configurator for a given store.
	 * 
	 * @param storeId the identity of the configuration store.
	 */
	public DefaultConfigurator(final String storeId) {
		_storeId = storeId;
	}

	@Override
	public void configure(final Configurable configurable) {
		_currentState.configure(configurable);
	}

	@Override
	public <T> T getAttribute(final String name, final T defaultValue) {
		Objects.requireNonNull(name, "'name' must not be null.");
		
		if (_attributes == null) {
			throw new IllegalStateException("Method called before configuration initialisation.");
		}
		
		final String fqdn = _storeId == null ? name : _storeId + "." + name;
		T value = (T) _attributes.get(fqdn);
		if (value == null) {
			value = (T) _attributes.get(name);
		}
		return value != null ? value : defaultValue;
	};	

	@Override
	public Set<String> getDeclaredIdentifiers() {
		if (_attributes != null) {
			final Set<String> identifiers = new TreeSet<String>();
			for (final Entry<String, Object> entry : _attributes.entrySet()) {
				final String attributeName = entry.getKey();
				final int indexOfDot = attributeName.indexOf(".");
				if (indexOfDot != -1) {
					identifiers.add(attributeName.substring(0, indexOfDot));
				}
			}
			return identifiers;
		}
		throw new IllegalStateException("Method called before configuration initialisation.");
	}

	@Override
	public String getOwningStoreIdentifier() {
		return _storeId;
	}

	/**
	 * Switches the current state of this configurator.
	 * 
	 * @param newState the new state.
	 * @param configurable the configurable target.
	 */
	void transitionTo(final ConfigurationState newState, final Configurable configurable) {
		_currentState = newState;
		_currentState.configure(configurable);
	}
}