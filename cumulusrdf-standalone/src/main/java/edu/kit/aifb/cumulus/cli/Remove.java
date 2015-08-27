package edu.kit.aifb.cumulus.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;

import edu.kit.aifb.cumulus.cli.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFSail;

/**
 * Removes all results from the given SPARQL query.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Remove extends Command {

	@Override
	Options getOptions() {
		final Option inputO = new Option("q", "sparql construct query string. all its bindings will be removed.");
		inputO.setArgs(1);
		inputO.setRequired(true);
		
		final Option verboseO = new Option("v", "verbose (default: false)");
		verboseO.setArgs(1);

		final Option storageO = new Option("s", "storage layout to use (triple|quad) (needs to match webapp configuration)");
		storageO.setArgs(1);

		final Option helpO = new Option("h", "print help");

		final Options options = new Options();
		options.addOption(inputO);
		options.addOption(storageO);
		options.addOption(helpO);
		
		return options;
	}
	
	@Override
	public void doExecute(final CommandLine commandLine, final Store store) {
		final String query = commandLine.getOptionValue("q");
		
		final boolean verbose = commandLine.hasOption("v");

		SailRepository repo = null;
		SailRepositoryConnection con = null;
		try {
			CumulusRDFSail sail = new CumulusRDFSail(store);
			sail.initialize();
	
			repo = new SailRepository(sail);
			con = repo.getConnection();
			final GraphQuery parsed_query = con.prepareGraphQuery(QueryLanguage.SPARQL, query);
	
			int counter = 0;
	
			for (final GraphQueryResult result = parsed_query.evaluate(); result.hasNext();) {
	
				final Statement stmt = result.next();
				con.remove(stmt);
				
				if (verbose) {
					_log.info(MessageCatalog._00024_DELETED_STATEMENT, stmt);
				}

				counter++;
			}
			
			_log.info(MessageCatalog._00025_DELETE_REPORT, counter);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			return;
		} finally {
			if (con != null) {
				// CHECKSTYLE:OFF
				try { con.close(); } catch (Exception ignore) {};	
				// CHECKSTYLE: ON
			}
			
			if (repo != null) {
				// CHECKSTYLE:OFF
				try { repo.shutDown(); } catch (Exception ignore) {};	
				// CHECKSTYLE: ON
			}			
		}
	}
}