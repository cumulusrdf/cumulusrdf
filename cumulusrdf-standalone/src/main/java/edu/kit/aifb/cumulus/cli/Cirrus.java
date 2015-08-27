package edu.kit.aifb.cumulus.cli;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.ParseException;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.cli.log.MessageCatalog;
import edu.kit.aifb.cumulus.log.Log;

/**
 * Main class for the CLI, directs the call to one of the other CLI commands.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class Cirrus {
	static final Log LOG = new Log(LoggerFactory.getLogger(Cirrus.class));
	
	private static final Map<String, Command> COMMAND_REGISTRY = new HashMap<String, Command>();
	static {
		COMMAND_REGISTRY.put("load", new Load());
		COMMAND_REGISTRY.put("dump", new Dump());
		COMMAND_REGISTRY.put("query", new Query());
		COMMAND_REGISTRY.put("remove", new Remove());
	}

	/**
	 * Cirrus main entry point.
	 * 
	 * @param args the command line arguments.
	 */
	public static void main(final String[] args) {
		
		if (args.length < 1) {
			LOG.error(MessageCatalog.WRONG_ARGS_SIZE);
			System.exit(1);
		}

		final Command command = COMMAND_REGISTRY.get(args[0]);
		if (command == null) {
			LOG.error(MessageCatalog._00001__UNKNOWN_COMMAND, args[0]);
			System.exit(1);			
		}
		
		final CommandLineParser parser = new BasicParser();

		try {
			final CommandLine commandLine = parser.parse(command.getOptions(), args);
			command.execute(commandLine);
		} catch (final MissingOptionException exception) {
			command._log.error(MessageCatalog._00028_CL_PARSER_FAILURE, exception.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("cirrus <command> <options>", command.getOptions());
			System.exit(1);						
		} catch (final ParseException exception) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("cirrus <command> <options>", command.getOptions());
			System.exit(1);			
		}
	}
}