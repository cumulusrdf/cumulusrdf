package edu.kit.aifb.cumulus.cli;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;

import edu.kit.aifb.cumulus.cli.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.QuadStore;
import edu.kit.aifb.cumulus.store.Store;

/**
 * Dumps the stored triples as NTriples file.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Dump extends Command{
	
	@Override
	Options getOptions() {
		final Option inputO = new Option("o", "name of output file");
		inputO.setArgs(1);
		inputO.setRequired(true);
		
		final Option inputS = new Option("s", "Storage layout (e.g. triple or quad; defaults to triple)");
		inputS.setArgs(1);

		final Option batchF = new Option("f", "all common RDF formats (e.g., 'n-triples', 'n-quads', or 'rdf/xml') (default: 'n-triples')");
		batchF.setArgs(1);

		final Option helpO = new Option("h", "print help");

		final Options options = new Options();
		options.addOption(inputO);
		options.addOption(batchF);
		options.addOption(helpO);
		return options;
	}

	@Override
	public void doExecute(final CommandLine commandLine, final Store store) {
		final String outputFilePath = commandLine.getOptionValue("o");
		
		final File outputFile = new File(outputFilePath);
		if (!outputFile.canWrite()) {
			_log.error(
					MessageCatalog._00003_BAD_OUTPUTFILE, 
					outputFile.getAbsolutePath());			
		}
		
		final String format = commandLine.hasOption("f")
				? commandLine.getOptionValue("f")
				: "N-Quads";

		final RDFFormat rdfFormat = RDFFormat.valueOf(format);

		if (rdfFormat == null) {
			_log.error(MessageCatalog._00002_BAD_FORMAT, format);
			return;
		}

		long start = System.currentTimeMillis();
		int counter = 0;
		BufferedWriter outputWriter = null;
		try {
			outputWriter = new BufferedWriter(new FileWriter(outputFile));
			final RDFWriter rdfWriter = Rio.createWriter(rdfFormat, outputWriter);
			
			_log.info(MessageCatalog._00013_DUMP_STARTS, outputFile.getAbsolutePath());
	
			final boolean isQuad = store instanceof QuadStore;
			
			rdfWriter.startRDF();
			
			for (final Iterator<Statement> iter = store.query(
					isQuad 
						? new Value[] { null, null, null, null } 
						: new Value[] { null, null, null }); iter.hasNext();) {
	
				rdfWriter.handleStatement(iter.next());
	
				if (++counter % 10000 == 0) {
					_log.info(MessageCatalog._00014_DUMP_CHUNK_DEBUG, counter);
				}
			}
			
			rdfWriter.endRDF();
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			return;
		} finally {
			if (outputWriter != null) {
				// CHECKSTYLE:OFF
				try { outputWriter.flush(); } catch (Exception ignore) {};	
				try { outputWriter.close(); } catch (Exception ignore) {};	
				// CHECKSTYLE: ON
			}
		}
		
		if (counter != 0) {
			_log.info(
					MessageCatalog._00015_DUMP_REPORT, 
					counter, 
					outputFile.getAbsolutePath(),
					((start - System.currentTimeMillis()) / 1000d));
		} else {
			_log.info(MessageCatalog._00027_EMPTY_DUMP_REPORT);			
		}
	}
}