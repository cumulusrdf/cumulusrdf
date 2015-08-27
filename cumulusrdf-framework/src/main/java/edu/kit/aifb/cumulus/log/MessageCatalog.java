package edu.kit.aifb.cumulus.log;

/**
 * CumulusRDF message catalog.
 * Basically an interface that simply enumerates all log messages.
 * Note that constants ending with _MSG are supposed to be sent only to clients (not in log).
 * 
 * @author Andrea Gazzarini
 * @since 1.1
 */
public interface MessageCatalog {
	String PREFIX = "<CRDF";
	String _00001_CRDF_STARTUP = PREFIX + "-00001> : Startup procedure for CumulusRDF has been initiated.";
	String _00002_CONFIG_FILE_PARAM = PREFIX + "-00002> : Found config-file parameter : %s";
	String _00003_LOAD_CONFIG_FROM_CLASSPATH_RESOURCE = PREFIX + "-00003> : Loading CumulusRDF configuration from classpath resource %s";
	String _00004_CONFIGURATION_ATTRIBUTE = PREFIX + "-00004> : Adding / replacing configuration attribute: %s = %s";
	String _00005_LOAD_CONFIG_FROM_DEPLOYMENT_DESCRIPTOR = PREFIX + "-00005> : Loading CumulusRDF configuration from deployment descriptor.";
	String _00006_MANDATORY_CONFIG_ATTRIBUTE_MISSING = PREFIX + "-00006> : Missing one or more required configuration attributes %s";
	String _00007_ASSIGNING_DEFAULT_VALUES = PREFIX + "-00007> : Assigning default values to configuration attributes...";
	String _00008_UNKNOWN_STORAGE_LAYOUT = PREFIX + "-00008> : Unknown storage layout has been specified: %s";
	String _00009_REPOSITORY_INITIALIZED = PREFIX + "-00009> : CumulusRDF repository has been successfully initialized.";
	String _00010_CRDF_STARTED = PREFIX + "-00010> : CumulusRDF open for e-business";
	String _00011_REPOSITORY_INITIALISATION_FAILURE = PREFIX + "-00011> : Unable to initialize CumulusRDF repository. See below for further details.";
	String _00012_REPOSITORY_SHUTDOWN_START = PREFIX + "-00012> : CumulusRDF repository shutdown starts...";
	String _00013_REPOSITORY_SHUTDOWN_FAILURE = PREFIX + "-00013> : Unable to shutdown CumulusRDF repository.";
	String _00014_REPOSITORY_SHUTDOWN = PREFIX + "-00014> : CumulusRDF repository has been shutdown.";
	String _00015_CRDF_STARTED_WITH_FAILURES =
			PREFIX
					+ "-00015> : CumulusRDF has been started, however there were failures during startup initialization. "
					+ "As consequence of that, System is supposed to be unstable. Please check your configuration, log files and try to fix problems.";
	String _00016_CDRF_SHUTDOWN_START = PREFIX + "-00016> : Shutdown procedure for CumulusRDF has been initiated...";
	String _00017_CRDF_SHUTDOWN_END = PREFIX + "-00017> : CumulusRDF has been shutdown.";
	String _00018_EXPLICIT_STORE_SHUTDOWN_START = PREFIX + "-00018> : Explicit CumulusRDF store shutdown starts...";
	String _00019_EXPLICIT_STORE_SHUTDOWN_FAILURE = PREFIX + "-00019> : Unable to explicitly shutdown the CumulusRDF store...";
	String _00020_EXPLICIT_STORE_SHUTDOWN = PREFIX + "-00020> : CumulusRDF store has been explicitly shutdown.";
	String _00021_INVALID_CONFIG_CLASSPATH_RESOURCE = PREFIX + "-00021> : Classpath resource %s doesn't lead to a valid input stream.";
	String _00022_MALFORMED_URI = PREFIX + "-00022> : Received a malformed URI %s";
	String _00023_MISSING_URI = PREFIX + "-00023> : Missing mandatory parameter \"uri\"";
	String _00024_MISSING_REQUIRED_PARAM = PREFIX + "-00024> : Invalid request. At least one required parameter is missing. Query string was %s.";
	String _00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG = PREFIX + "-00025> : A system internal failure has been detected.";
	String _00025_CUMULUS_SYSTEM_INTERNAL_FAILURE = _00025_CUMULUS_SYSTEM_INTERNAL_FAILURE_MSG + " See below for further details.";
	String _00026_NWS_SYSTEM_INTERNAL_FAILURE = PREFIX + "-00026> : Not-well specified system internal failure has been detected. See below for further details.";
	String _00027_BODY_PARSING_FAILURE = PREFIX + "-00027> : System was unable to parse the body of incoming request. See below for further details.";
	String _00028_BODY_PARSING_RESULTS_IN_EMPTY_DATA = PREFIX + "-00028> : The POST request cannot be considered valid because body contains seems empty.";
	String _00029_RDF_PARSE_FAILURE = PREFIX + "-00029> : Unable to parse a given stream as a valid RDF. See below for further details.";
	String _00030_WEB_MODULE_IO_FAILURE = PREFIX + "-00030> : Detected an I/O failure while serving a resource. See below for further details.";
	String _00031_WEB_MODULE_SERVLET_FAILURE = PREFIX + "-00031> : Detected an servlet component failure while serving a resource. See below for further details.";

	String _00032_LIMIT_PARAM_NAN_MSG = PREFIX + "-00032> : Received an invalid (NaN) 'l' parameter : %s";
	String _00033_RESOURCE_NOT_FOUND_MSG = PREFIX + "-00033> : Resource not found.";
	String _00034_BAD_ACCEPT_MIME_TYPE = PREFIX + "-00034> : Unknown or unsupported mime-type for Accept header / param: %s";
	String _00035_MISSING_URI_OR_PATTERN = PREFIX + "-00035> : Missing URI or Pattern. At least one of them must be specified.";
	String _00036_NOT_MULTIPART_REQUEST = PREFIX + "-00036> : This service requires a multipart file upload request.";

	String _00037_BULK_DATA_SIZE = PREFIX + "-00037> : Bulk load service read %s bytes from file upload.";
	String _00038_BULK_DATA_READ = PREFIX + "-00038> : Bulk load service read %s bytes in total.";
	String _00039_NO_EXTENSION_FILE = PREFIX + "-00039> : Unable to proceed...no extension file given.";

	String _00040_PARSING_NTRIPLES_FILE = PREFIX + "-00040> : Parsing %s content as ntriples.";
	String _00041_PARSING_RDFXML_FILE = PREFIX + "-00041> : Parsing %s content as RDF/XML.";
	String _00042_PARSING_QUADS_FILE = PREFIX + "-00042> : Parsing %s content as quads.";
	String _00043_UNKNOWN_EXTENSION_FILE = PREFIX + "-00043> : Unknown or unsupported file extension: %s";
	String _00044_EMPTY_INPUT_FILE = PREFIX + "-00044> : Unable to proceed, file %s has no data.";
	String _00045_MISSING_QUERY_OR_UPDATE_PARAM = PREFIX + "-00045> : Unable to proceed, missing 'query' or 'update' parameter.";
	String _00046_BATCH_BULK_LOAD_DATA_STARTS = PREFIX + "-00046> : Batch bulk load service has been invoked with batchLimit = %s";
	String _00047_BATCH_BULK_LOAD_DATA_THREAD_POOL_SIZE = PREFIX + "-00047> : Store is running with a pool of %s bulk workers.";
	String _00048_BATCH_BULK_LOAD_IS_WORKING = PREFIX + "-00048> : Batch bulk load service waiting for threads to finish...";
	String _00049_STORE_ALREADY_OPEN = PREFIX + "-00049> : Store already open and connected to underlying storage.";
	String _00050_REPLICATION_FACTOR = PREFIX + "-00050> : Replication factor in use: %s";
	String _00051_CONSISTENCY_LEVEL = PREFIX + "-00051> : Consistency level in use: READ = %s, WRITE = %s";
	String _00052_STORE_OPEN = PREFIX + "-00052> : Store open and connected to underlying storage.";
	String _00053_STORE_LISTENERS = PREFIX + "-00053> : Store listeners registry: %s";
	String _00054_ESTIMATED_DELETION_SIZE = PREFIX + "-00054> : (Estimated) triples / quads deletion size: %s";
	String _00055_DELETION_FAILURE = PREFIX + "-00055> : System internal failure while deleting data. See below for further details.";
	String _00056_ESTIMATED_ADD_SIZE = PREFIX + "-00056> : (Estimated) triples / quads add size: %s";
	String _00057_ADD_FAILURE = PREFIX + "-00057> : System internal failure while adding data. See below for further details.";
	String _00058_DELETION_BY_ID_FAILURE = PREFIX + "-00058> : System internal failure while deleting data associated with id %s. See below for further details.";
	String _00059_BAD_DOUBLE_VALUE = PREFIX + "-00059> : Detected a bad double value (%s).";
	String _00060_BAD_LITERAL_VALUE = PREFIX + "-00060> : Detected a bad literal value (%s).";
	String _00061_QUERY_EVALUATION_FAILURE = PREFIX + "-00061> : Query evaluation failure (%s). See below for further details.";
	String _00062_QUERY_EVALUATION_FAILURE = PREFIX + "-00062> : Query evaluation failure (query = %s, LBound = %s, HBound = %s). See below for further details.";

	String _00063_CREATING_NEW_SCHEMA = PREFIX + "-00063> : Creating new schema...";
	String _00064_HOW_MANY_TYPE_TRIPLES = PREFIX + "-00064> : %s type-triples has been processed for this new schema.";
	String _00065_HOW_MANY_CLASSES_FOUND = PREFIX + "-00065> : %s classes has been found for this new schema.";
	String _00066_SAMPLING_OVER = PREFIX + "-00066> : Sampling over instances of %s class.";
	String _00067_HOW_MANY_INSTANCE_TRIPLES = PREFIX + "-00067> : %s instance-triples has been processed for this new schema.";
	String _00068_SAMPLING_COMPLETED = PREFIX + "-00068> : Completed sampling: iterated over %s triples";
	String _00069_SCHEMA_MSG_PREFIX = PREFIX + "-00069> : SCHEMA : %s";
	String _00070_UNKNOWN_ID_IN_DICTIONARY = PREFIX + "-00070> : Unable to find a value in Dictionary.";
	String _00071_UNABLE_TO_RESOLVE_COLLISION = PREFIX + "-00071> : Could not resolve collision for node %s after %s tries. Giving up and overwriting existing id.";
	String _00072_DATATYPE_BOUNDS_MISMATCH = PREFIX + "-00072> : Upper %s and Lower %s bound datatypes must match!";
	String _00073_DATATYPE_BOUNDS_NULL = PREFIX + "-00073> : Cannot perform optimized range query with upper and lower bound being null!";
	String _00074_BOUND_NOT_LITERAL = PREFIX + "-00074> : Bound is not a literal.";
	String _00075_COULDNT_LOAD_NODE = PREFIX + "-00075> : Could not load node %s. See below for further details.";
	String _00076_INSERT_STATS = PREFIX + "-00076> : %s triples inserted in %s ms (%s triples/s)";
	String _00077_LOAD_RDF_DATA_FAILURE = PREFIX + "-00077> : Unable to load RDF data. See below for further details.";
	String _00078_DELETE_WATCH = PREFIX + "-00078> ";
	String _00079_ADD_WATCH = PREFIX + "-00079> ";
	String _00080_INIT_COUNTER = PREFIX + "-00080> : Init counter: %s @ value: %s";
	String _00081_COUNTER_OVERFLOW = PREFIX + "-00081> : Counter %s suffered an overflow! Current counter value: %s";
	String _00082_NEW_LOAD_WORKER = PREFIX + "-00082> : Creating new bulk load worker with id %s";
	String _00083_TOTAL_INSERT_STATS = PREFIX + "-00083> : Bulk job has been completed: %s triples have been inserted in %s ms (%s triples/s)";
	String _00084_STORE_ALREADY_CLOSED = PREFIX + "-00084> : Store instance %s is already closed.";
	String _00085_STORE_HAS_BEEN_CLOSED = PREFIX + "-00085> : Store instance %s has been closed.";
	String _00086_NODE_NOT_FOUND_IN_DICTIONARY = PREFIX + "-00086> : Node %s not found in dictionary.";
	String _00087_INSERT_FAILURE = PREFIX + "-00087> : Failed to insert batch of %s dictionary entries.";
	String _00088_VALUE_CREATE_FAILURE = PREFIX + "-00088> : Unable to create a valid Value instance from id %s";
	String _00089_DICTIONARY_IN_USE = PREFIX + "-00089> : Dictionary in use %s";
	String _00090_DICTIONARY_INITIALISED = PREFIX + "-00090> : Dictionary %s has been initialised.";
	String _00091_NULL_DECORATEE_DICT = PREFIX + "-00091> : Invalid (null) decoratee dictionary.";
	String _00092_DICTIONARY_INIT_FAILURE = PREFIX + "-00092> : System was unable to initialise dictionary. See below for further details.";
	String _00093_DATA_ACCESS_LAYER_FAILURE = PREFIX + "-00093> : Data access failure. See below for further details.";
	String _00094_SCHEMA_INIT_FAILURE = PREFIX + "-00094> : System was unable to initialise schema. See below for further details.";
	String _00095_COUNTER_FACTORY_INIT_FAILURE = PREFIX + "-00095> : System was unable to initialise counter factory. See below for further details.";
	String _00096_PREFIX_2_NS_INIT_FAILURE = PREFIX + "-00096> : System was unable to initialise prefix 2 namespace. See below for further details.";
	String _00098_UNABLE_TO_INSTANTIATE_DAL_FACTORY = PREFIX + "-00098> : Unable to instantiate DataAccessLayerFactory %s. See below for further details.";
	String _00099_CONFIG_FILE_NOT_FOUND = PREFIX + "-00099> : Unable to find CumulusRDF configuration file %s.";
	String _00100_CONFIG_FILE_NOT_READABLE = PREFIX + "-00100> : Unable to read CumulusRDF configuration file %s. The file must exists and must be readable.";
	String _00101_USING_CONFIG_FILE = PREFIX + "-00101> : CumulusRDF will be configured with configuration file %s";
	String _00102_USING_CLASSPATH_RESOURCE = PREFIX + "-00102> : CumulusRDF will be configured with classpath resource /%s";
	String _00103_USING_EMBEDDED_CONFIGURATION = PREFIX + "-00103> : CumulusRDF will be configured with embedded configuration (%s)";
	String _00104_UTF8_NOT_SUPPORTED = PREFIX + "-00104> : UTF-8 encoding not supported.";
	String _00105_COULD_NOT_GET_HASH = PREFIX + "-00105> : Could not get hash for value: %s";
	String _00106_ERROR_FETCHING_QUERY_RESULT = PREFIX + "-00106> : Error fetching query result.";
	String _00107_CONNECTED_TO_CLUSTER = PREFIX + "-00107> : Connected to Cassandra cluster.";
	String _00108_DISCONNECTED_FROM_CLUSTER = PREFIX + "-00108> : Disconnected from Cassandra cluster.";
	String _00109_UNABLE_TO_REGISTER_MBEAN = PREFIX + "-00109> : Unable to register the management interface with name #%s.";
	String _00110_MBEAN_REGISTERED = PREFIX + "-00110> : Management Interface for #%s has been registered with Management Server.";
	String _00111_MBEAN_ALREADY_REGISTERED = PREFIX + "-00111> : A Management Interface with ID #%s already exists on Management Server.";
	String _00112_MBEAN_UNREGISTERED = PREFIX + "-00112> : Management Interface with ID #%s has been unregistered from Management Server.";
	String _00113_UNABLE_TO_UNREGISTER_MBEAN = PREFIX + "-00113> : Unable to unregister the management interface with name #%s.";
	String _00114_UNDERLYING_STORAGE = PREFIX + "-00114> : Store #%s, underlying storage: %s";
	
	String _00115_WEB_MODULE_REQUEST_NOT_VALID = PREFIX + "-00115> : Servlet request was not valid.";
}