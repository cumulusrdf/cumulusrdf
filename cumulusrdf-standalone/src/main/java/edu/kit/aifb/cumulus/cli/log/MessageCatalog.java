package edu.kit.aifb.cumulus.cli.log;

/**
 * CumulusRDF message catalog.
 * Basically an interface that simply enumerates all log messages.
 * Note that constants ending with _MSG are supposed to be sent only to clients (not in log).
 * 
 * @author Andrea Gazzarini
 * @since 1.1
 */
public interface MessageCatalog {
	String PREFIX = "<CIRRUS";
	String AVAILABLE_COMMANDS = 
			"\tload\t\tLoad and index triples/quads" 
			+ "\n\tquery\t\tQuery the store" 
			+ "\n\tremove\t\tDelete triples/quads" 
			+ "\n\tdump\t\tDump the data";

	String WRONG_ARGS_SIZE = 
			"No command has been specified. Available commands are:\n" 
			+ AVAILABLE_COMMANDS; 
		
	String _00001__UNKNOWN_COMMAND = PREFIX + "-00001> : Unknown command %s. Available commands are:\n" + AVAILABLE_COMMANDS;
	String _00002_BAD_FORMAT = PREFIX + "-00002> : Unknown format has been specified: %s";
	String _00003_BAD_OUTPUTFILE = PREFIX + "-00003> : Invalid output file %s. Check if the path exists and if your user has write permissions on that path.";
	String _00004_CONFIGURATION_ATTRIBUTE = PREFIX + "-00004> : Adding / replacing configuration attribute: %s = %s";
	String _00005_USING_TRIPLE_STORE_LAYOUT = PREFIX + "-00005> : Using Triple Store layout.";
	String _00006_USING_QUAD_STORE_LAYOUT = PREFIX + "-00006> : Using Quad Store layout.";
	String _00007_OPENING_STORE = PREFIX + "-00007> : Opening store...";
	String _00008_STORE_OPEN = PREFIX + "-00008> : Store is open.";
	String _00009_CLOSING_STORE = PREFIX + "-00009> : Closing store...";
	String _00010_STORE_CLOSED = PREFIX + "-00009> : Store closed.";
	String _00011_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG = PREFIX + "-00025> : A system internal failure has been detected.";
	String _00012_CUMULUS_SYSTEM_INTERNAL_FAILURE = _00011_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG + " See below for further details.";
	String _00013_DUMP_STARTS = PREFIX + "-00013> : Starts writing to file %s";
	String _00014_DUMP_CHUNK_DEBUG = PREFIX + "-00014> : Dump in progress: Wrote %s triples in total.";
	String _00015_DUMP_REPORT = PREFIX + "-00015> : Dump completed. Wrote %s triples to file %s in %s seconds.";
	String _00016_BAD_INPUTFILE = PREFIX + "-00016> : Invalid input file %s. Check if the path exists and if your user has read permissions on that path.";
	String _00017_CANNOT_FIND_PARSER = PREFIX + "-00017> : Cirrus was unable to find a suitable parser for file %s.";
	String _00018_LOAD_REPORT = PREFIX + "-00018> : Load completed. Insertion took %s msecs.";
	String _00019_PARSED_ASK_QUERY = PREFIX + "-00019> : ASK query: %s";
	String _00020_PARSED_ASK_ANSWER = PREFIX + "-00020> : ASK answer: %s";
	String _00021_PARSED_SELECT_ANSWER = PREFIX + "-00021> : SELECT answer:";
	String _00022_CONSTRUCT_ASK_QUERY = PREFIX + "-00022> : CONSTRUCT query: %s";
	String _00023_CONSTRUCT_ASK_ANSWER = PREFIX + "-00023> : CONSTRUCT answer:";
	String _00024_DELETED_STATEMENT = PREFIX + "-00024> : Deleted triple %s";
	String _00025_DELETE_REPORT = PREFIX + "-00025> : Delete completed. %s triples have been deleted in total.";

	String _00026_NWS_SYSTEM_INTERNAL_FAILURE = PREFIX + "-00026> : Not-well specified system internal failure has been detected. See below for further details.";
	String _00027_EMPTY_DUMP_REPORT = PREFIX + "-00027> : Dump completed. Howewer, the store is empty.";
	String _00028_CL_PARSER_FAILURE = PREFIX + "-00028> : %s";
}