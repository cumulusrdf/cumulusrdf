package edu.kit.aifb.cumulus.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.cli.log.MessageCatalog;
import edu.kit.aifb.cumulus.framework.Environment.ConfigValues;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.store.CumulusStoreException;
import edu.kit.aifb.cumulus.store.QuadStore;
import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.TripleStore;

/**
 * Supertype layer for all CumulusRDF command line commands.
 * Provides a common behaviour that is reused among all concrete commands.
 * 
 * @see http://it.wikipedia.org/wiki/Template_method
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public abstract class Command {
	protected final Log _log = new Log(LoggerFactory.getLogger(Cirrus.class));
	
	/**
	 * Executes this {@link Command}.
	 * 
	 * @param commandLine the current command line.
	 */
	public final void execute(final CommandLine commandLine) {
		if (commandLine.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("", getOptions());
			return;
		}
		
		Store store = null;
		if (commandLine.hasOption("s")) {
			final String layout = commandLine.getOptionValue("s");
			if (ConfigValues.STORE_LAYOUT_QUAD.equalsIgnoreCase(layout)) {
				store = new QuadStore();
				_log.info(MessageCatalog._00006_USING_QUAD_STORE_LAYOUT);
			} else {
				store = new TripleStore();
				_log.info(MessageCatalog._00005_USING_TRIPLE_STORE_LAYOUT);
			} 
		} else {
			store = new TripleStore();
			_log.info(MessageCatalog._00005_USING_TRIPLE_STORE_LAYOUT);
		}

		try {
			store.open();

			doExecute(commandLine, store);

		} catch (final CumulusStoreException exception) {
			_log.error(MessageCatalog._00012_CUMULUS_SYSTEM_INTERNAL_FAILURE, exception);
		} finally {
			try {
				store.close();
			} catch (Exception exception) {
				_log.error(MessageCatalog._00012_CUMULUS_SYSTEM_INTERNAL_FAILURE, exception);
			}
		}
	}
	
	/**
	 * Primitive operation that defines the concrete command behaviour.
	 * 
	 * @param commandLine the current command line.
	 * @param store the RDF store in use.
	 */
	public abstract void doExecute(CommandLine commandLine, Store store);
	
	/**
	 * Returns the options associated with this command.
	 * Each concrete command must define here all available options.
	 * 
	 * @return the options associated with this command.
	 */
	abstract Options getOptions();
}