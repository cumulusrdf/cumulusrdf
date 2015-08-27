package edu.kit.aifb.cumulus.cli;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.openrdf.rio.RDFFormat;

import edu.kit.aifb.cumulus.cli.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.Store;

/**
 * 
 * Loads triples or quads from a file and stores them into a cumulusrdf store.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini.
 * @since 1.0
 */
public class Load extends Command {

	@Override
	Options getOptions() {
		final Option inputO = new Option("i", "Name of file to read");
		inputO.setRequired(true);
		inputO.setArgs(1);

		final Option storageO = new Option("s", "Storage layout to use (triple|quad)");
		storageO.setArgs(1);

		final Option batchO = new Option("b", "Batch size - number of triples (default: 1000)");
		batchO.setArgs(1);

		final Option helpO = new Option("h", "Print help");

		final Options options = new Options();
		options.addOption(inputO);
		options.addOption(storageO);
		options.addOption(batchO);
		options.addOption(helpO);
		
		return options;
	}
	
	@Override
	public void doExecute(final CommandLine commandLine, final Store store) {

		final String inputFilePath = commandLine.getOptionValue("i");
		final File inputFile = new File(inputFilePath);
	
		if (!inputFile.canRead()) {
			_log.error(MessageCatalog._00016_BAD_INPUTFILE, inputFile.getAbsolutePath());
			return;
		}
		
		final RDFFormat rdfFormat = RDFFormat.forFileName(inputFile.getName());

		if (rdfFormat == null) {
			_log.error(MessageCatalog._00017_CANNOT_FIND_PARSER, inputFile.getName());
			return;
		}

		int batchSize = 1000;
		if (commandLine.hasOption("b")) {
			try {
				batchSize = Integer.parseInt(commandLine.getOptionValue("b"));	
			} catch (final Exception exception) {
				// Ignore: just use the default value
			}
			
		}

		store.setDefaultBatchLimit(batchSize);

		long start = System.nanoTime();
		try {
			store.bulkLoad(inputFile, rdfFormat);

			double duration = (System.nanoTime() - start) / 1e9;
			_log.info(MessageCatalog._00018_LOAD_REPORT, duration);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
		} 
	}
}