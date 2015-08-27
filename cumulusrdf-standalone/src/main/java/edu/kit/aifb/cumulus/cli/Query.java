package edu.kit.aifb.cumulus.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;

import edu.kit.aifb.cumulus.cli.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFSail;

/**
 * 
 * Executes a SPARQL query and prints out all results.
 * 
 * @author Andreas Wagner
 * @since 1.0
 */
public class Query extends Command {

	@Override
	Options getOptions() {
		final Option inputO = new Option("q", "sparql query string");
		inputO.setRequired(true);
		inputO.setArgs(1);

		final Option storageO = new Option("s", "storage layout to use (triple|quad)");
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

		SailRepositoryConnection con = null;
		SailRepository repo = null;
		try {
			final CumulusRDFSail sail = new CumulusRDFSail(store);
			sail.initialize();

			repo = new SailRepository(sail);
			con = repo.getConnection();
			org.openrdf.query.Query parsed_query = con.prepareQuery(QueryLanguage.SPARQL, query);

			int i = 0;
	
			if (parsed_query instanceof BooleanQuery) {
				_log.info(MessageCatalog._00019_PARSED_ASK_QUERY, parsed_query);
				_log.info(MessageCatalog._00020_PARSED_ASK_ANSWER, ((BooleanQuery) parsed_query).evaluate());
			} else if (parsed_query instanceof TupleQuery) {
	
				_log.info(MessageCatalog._00021_PARSED_SELECT_ANSWER);
				for (final TupleQueryResult result = ((TupleQuery) parsed_query).evaluate(); result.hasNext(); i++) {
					_log.info(i + ": " + result.next());
				}
			} else if (parsed_query instanceof GraphQuery) {
				_log.info(MessageCatalog._00022_CONSTRUCT_ASK_QUERY, parsed_query);
				_log.info(MessageCatalog._00023_CONSTRUCT_ASK_ANSWER);
				for (final GraphQueryResult result = ((GraphQuery) parsed_query).evaluate(); result.hasNext(); i++) {
					_log.info(i + ": " + result.next());
				}
			}
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