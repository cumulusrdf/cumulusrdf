package edu.kit.aifb.cumulus.framework.domain.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * A configurator that allows to override configuration values from the config files.
 * 
 * @author Sebastian Schmidt
 * @since 1.1.0
 */
@SuppressWarnings("unchecked")
public class ProgrammableConfigurator extends DefaultConfigurator {
	
	final ConfigurationState _configurationHasBeenLoaded = new ConfigurationState() {

		@Override
		public void configure(final Configurable configurable) {
			configurable.accept(ProgrammableConfigurator.this);
		}
	};

	protected ConfigurationState _currentState = _configurationHasBeenLoaded;

	/**
	 * Creates a new programmable configurator that overrides the given attributes.
	 * 
	 * @param attributes the attributes to override.
	 */
	public ProgrammableConfigurator(final Map<String, ? extends Object> attributes) {
		
		super();
		
		_attributes = new HashMap<String, Object>();
		_attributes.putAll(attributes);
	}
	
	/**
	 * Creates a new programmable configurator that overrides the given attributes.
	 * 
	 * @param attributes the attributes to override.
	 */
	public ProgrammableConfigurator(String store_id, final Map<String, ? extends Object> attributes) {
		
		super(store_id);
		
		_attributes = new HashMap<String, Object>();
		_attributes.putAll(attributes);
	}
	
	@Override
	public <T> T getAttribute(final String name, final T defaultValue) {
		T value = (T) _attributes.get(name);
		
		if (value != null) {
			return value;
		} else {
			return super.getAttribute(name, defaultValue);
		}
	}
}