package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.framework.Environment.BASE_URI;
import static edu.kit.aifb.cumulus.framework.Environment.DATETIME_RANGETYPES;
import static edu.kit.aifb.cumulus.framework.Environment.NUMERIC_RANGETYPES;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.EventObject;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceAlreadyExistsException;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.AtomicDouble;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.datasource.StorageLayout;
import edu.kit.aifb.cumulus.framework.datasource.TripleIndexDAO;
import edu.kit.aifb.cumulus.framework.domain.configuration.Configurable;
import edu.kit.aifb.cumulus.framework.domain.configuration.Configuration;
import edu.kit.aifb.cumulus.framework.domain.configuration.DefaultConfigurator;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.framework.domain.selectivity.ISelectivityEstimator;
import edu.kit.aifb.cumulus.framework.events.ITriplesChangesListener;
import edu.kit.aifb.cumulus.framework.mx.ManageableStore;
import edu.kit.aifb.cumulus.framework.mx.ManagementRegistrar;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.events.AddTripleEvent;
import edu.kit.aifb.cumulus.store.events.FinishedChangesEvent;
import edu.kit.aifb.cumulus.store.events.StartChangesEvent;
import edu.kit.aifb.cumulus.store.sel.HeuristicsBasedSelectivityEstimator;
import edu.kit.aifb.cumulus.util.Util;

/**
 * Supertype lauer for CumulusRDF stores.
 * 
 * @author Andreas Wagner
 * @author Andreas Harth
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class Store implements Configurable<Map<String, Object>>, ManageableStore {
	/**
	 * Listener inteface for components interested in store status changes.
	 * 
	 * @author Andreas Wagner
	 * @since 1.0
	 */
	class StatusListener implements ITriplesChangesListener {

		private int _runningOperations;

		/**
		 * Returns true if there are running operations.
		 * @return True if there are running operations.
		 */
		public boolean active() {
			return _runningOperations > 0;
		}

		@Override
		public void update(final EventObject event) {
			if (event instanceof StartChangesEvent) {
				_runningOperations++;
			} else if (event instanceof FinishedChangesEvent) {
				_runningOperations--;
			}
		}
	}

	/**
	 * A simple value object that encapsulates a change operation result.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.0.1
	 */
	final class ChangeStatResult {
		private final long _elapsed;
		private final int _affectedCount;

		/**
		 * Builds a new result with the given data.
		 * 
		 * @param elapsed the elapsed time of the change.
		 * @param affectedCount how many affected triple / quads by the change.
		 */
		ChangeStatResult(final long elapsed, final int affectedCount) {
			this._elapsed = elapsed;
			this._affectedCount = affectedCount;
		}
	}

	/**
	 * RDFHandler implementor for bulk data load. 
	 * The load of data is actually done in chunks, in order to buffer and speedup the
	 * entire process. Callers can specify the size of those chunks with the batchLimit 
	 * parameter. 
	 * 
	 * @author Sebastian Schmidt
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	final class RDFBulkLoadHandler extends RDFHandlerBase {

		private final Queue<Future<ChangeStatResult>> _futures;
		private final int _batchLimit;

		private List<Statement> _statementBuffer;

		private long _totalElapsed;
		private int _estimatedCountOfInsertedStatements;

		private boolean _dequeuerEnabled = true;
		private Future<?> _dequeuerFuture;

		private final Runnable _dequeuer = new Runnable() {

			@Override
			public void run() {
				while (_dequeuerEnabled) {
					try {
						final Future<ChangeStatResult> future = _futures.poll();

						if (future != null) {
							final ChangeStatResult result = future.get();
							_estimatedCountOfInsertedStatements += result._affectedCount;
							final double ts = ((double) result._affectedCount / result._elapsed * 1000);
							_loadWorkerThroughput.set(ts);
							_log.debug(
									MessageCatalog._00076_INSERT_STATS,
									result._affectedCount,
									result._elapsed,
									ts);
						}
					} catch (final Exception exception) {
						_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
					}
				}
			}
		};

		/**
		 * Creates a new handler with the given threads.
		 * 
		 * @param batchLimit the maximum size of a chunk.
		 */
		public RDFBulkLoadHandler(final int batchLimit) {
			this._batchLimit = batchLimit;
			this._futures = new ConcurrentLinkedQueue<Future<ChangeStatResult>>();
		}

		@Override
		public void startRDF() throws RDFHandlerException {
			_dequeuerFuture = _workers.submit(_dequeuer);
			_statementBuffer = new ArrayList<Statement>(_batchLimit);
			_totalElapsed = System.currentTimeMillis();
		}

		@Override
		public void endRDF() throws RDFHandlerException {
			if (_dequeuerEnabled) {
				// Terminate dequeuer
				_dequeuerEnabled = false;

				try {
					// Wait until the dequeuer completes before continuing...
					// This is necessary to ensure that the bulk loading has really been completed once the bulkLoad method returns.
					// If we wouldn't do that, this method might return while the dequeuer is still executing.
					_dequeuerFuture.get();
				} catch (InterruptedException e) {
					_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, e);
				} catch (ExecutionException e) {
					_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, e);
				}

				if (_statementBuffer.size() > 0) {
					fireAddForCurrentChunk();
				}

				for (final Future<ChangeStatResult> future : _futures) {
					try {
						final ChangeStatResult result = future.get();
						_estimatedCountOfInsertedStatements += result._affectedCount;
						final double ts = ((double) result._affectedCount / result._elapsed * 1000);
						_loadWorkerThroughput.set(ts);
						_log.debug(
								MessageCatalog._00076_INSERT_STATS,
								result._affectedCount,
								result._elapsed,
								ts);
					} catch (final Exception exception) {
						_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
					}
				}

				_totalElapsed = System.currentTimeMillis() - _totalElapsed;
				final double loadThroughput = ((double) _estimatedCountOfInsertedStatements / _totalElapsed * 1000);
				_loadThroughput.set(loadThroughput);
				_log.info(
						MessageCatalog._00083_TOTAL_INSERT_STATS, 
						_estimatedCountOfInsertedStatements, 
						_totalElapsed,
						loadThroughput);
			}
		}

		@Override
		public void handleStatement(final Statement statement) throws RDFHandlerException {
			_statementBuffer.add(statement);

			if (_statementBuffer.size() >= _batchLimit) {
				fireAddForCurrentChunk();
			}
		}

		/**
		 * Schedules a new asynchronous task for inserting the current chunk.
		 */
		private void fireAddForCurrentChunk() {
			_futures.offer(_workers.submit(new BulkInsertCallable(_statementBuffer)));
			_statementBuffer.clear();
		}
	}

	/**
	 * A {@link Callable} implementation for defining a bulk insert task.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.0.1
	 */
	class BulkInsertCallable implements Callable<ChangeStatResult> {
		private final Iterator<Statement> _statements;
		private final int _statementsCount;

		/**
		 * Builds a new task for a given list of statements.
		 * 
		 * @param statements the statement that will be inserted.
		 */
		public BulkInsertCallable(final List<Statement> statements) {
			_statementsCount = statements.size();
			_statements = new ArrayList<Statement>(statements).iterator();
		}

		/**
		 * Executes the chunk insert.
		 * 
		 * @return a stat object containing (estimated) inserted triples and elapsed time.
		 * @throws Exception if something goes wrong during the insertion.
		 */
		@Override
		public ChangeStatResult call() throws Exception {
			long begin = System.currentTimeMillis();
			batchInsert(_statements, Integer.MAX_VALUE);
			return new ChangeStatResult(System.currentTimeMillis() - begin, _statementsCount);
		}
	}

	/**
	 * Iterator used in DESCRIBE.
	 * 
	 * @author Andreas Wagner
	 * @since 1.0
	 */
	public class DescribeIterator extends AbstractIterator<Statement> {

		private final Value _mResource;
		private final boolean _mInclude2Hop;

		// iterator for pattern with resource as subject
		private final Iterator<Statement> _mSit;
		// iterator for hop patterns
		private Iterator<Statement> _mHit;
		// iterator for pattern with resource as object
		private final Iterator<Statement> _mOit;
		private int _mSubjects;
		private int _mObjects;

		/**
		 * Builds a new Describe iterator with the given data.
		 * 
		 * @param resource the target resource.
		 * @param include2Hop 
		 * @param subjects max subjects in results.
		 * @param objects max objects in results.
		 * @throws CumulusStoreException in case of CumulusRDF internal failure.
		 */
		public DescribeIterator(final Value resource, final boolean include2Hop, final int subjects, final int objects) throws CumulusStoreException {

			_mResource = resource;
			_mInclude2Hop = include2Hop;
			_mSubjects = subjects;
			_mObjects = objects;

			_mSit = query(new Value[] {_mResource, null, null}, _mSubjects);
			_mOit = query(new Value[] {null, null, _mResource}, _mObjects);
		}

		@Override
		protected Statement computeNext() {

			if (_mHit != null && _mHit.hasNext()) {
				return _mHit.next();
			}

			if (_mSit != null && _mSit.hasNext()) {

				final Statement statement = _mSit.next();
				final Value object = statement.getObject();
				// if the hop should be included, prime the hop iterator,
				// pattern has the current object as subject
				if (_mInclude2Hop || object instanceof BNode) {
					try {
						_mHit = query(new Value[] {object, null, null}, _mSubjects);
					} catch (CumulusStoreException e) {
						_log.error(e.getMessage());
						_mHit = null;
					}
				}

				return statement;
			}

			if (_mOit != null && _mOit.hasNext()) {

				final Statement statement = _mOit.next();
				final Value object = statement.getObject();
				// if the hop should be included, prime the hop iterator,
				// pattern has the current object as subject
				if (_mInclude2Hop || object instanceof BNode) {
					try {
						_mHit = query(new Value[] {object, null, null}, _mSubjects);
					} catch (CumulusStoreException e) {
						_log.error(e.getMessage());
						_mHit = null;
					}
				}

				return statement;
			}

			return endOfData();
		}
	}

	static final Object DUMMY_SOURCE_EVENT = new Object();

	protected final Log _log = new Log(LoggerFactory.getLogger(getClass()));
	protected final String _id;
	protected TripleIndexDAO _rdfIndexDAO;
	protected ExecutorService _workers;
	protected int _batchLimit = 1000;
	protected boolean _idxRanges;

	protected int _insertRequests;
	protected int _deleteRequests;
	protected int _averageInsertedTriplesPerSec;
	protected int _averageDeletedTriplesPerSec;
	boolean _isOpen;

	protected CounterFactory _counterFactory;

	protected List<ITriplesChangesListener> _changeListeners;
	protected ISelectivityEstimator _selectEstimator;

	ITopLevelDictionary _dictionary;
	DataAccessLayerFactory _factory;

	protected Schema _schema;
	protected StatusListener _status;

	final Configuration<Map<String, Object>> _configurator;	
	
	protected StartChangesEvent _startChangesEvent = StartChangesEvent.newEvent(this);
	protected FinishedChangesEvent _finishedChangesEvent = FinishedChangesEvent.newEvent(this);

	protected AtomicDouble _loadWorkerThroughput = new AtomicDouble();
	protected AtomicDouble _loadThroughput = new AtomicDouble();
	
	protected AtomicDouble deletedTriplesPerSecond = new AtomicDouble();

	/**
	 * Builds a new anonymous (without identifier) store.
	 */
	public Store() {
		this(UUID.randomUUID().toString());
	}

	/**
	 * Builds a new store with a given identifier.
	 * 
	 * @param id the store identifier.
	 */
	public Store(final String id) {
		_id = id;
		_configurator = new DefaultConfigurator(id);
	}

	@Override
	public String getId() {
		return _id;
	}

	@Override
	public boolean isOpen() {
		return _isOpen;
	}

	/**
	 * Returns the counter factory associated with this store.
	 * 
	 * @return the counter factory associated with this store.
	 */
	public CounterFactory getCounterFactory() {
		_counterFactory = CounterFactory.getInstance();
		return _counterFactory;
	}
	
	/**
	 * Returns the dictionary that is used to translate from ids to the
	 * corresponding nodes and back.
	 * 
	 * @return The node dictionary.
	 */
	public ITopLevelDictionary getDictionary() {
		return _dictionary;
	}
	
	/**
	 * Returns the selectivity estimator that is used by this store.
	 * 
	 * @return selectivity estimator
	 */
	public ISelectivityEstimator getSelectivityEstimator() {
		return _selectEstimator;
	}
	
	@Override
	public int hashCode() {
		return _id.hashCode();
	}

	@Override
	public double getLoadWorkerThroughput() {
		return _loadWorkerThroughput.get();
	}
	
	@Override
	public double getLoadThroughput() {
		return _loadThroughput.get();
	}	

	@Override
	public boolean equals(final Object anotherStore) {
		return anotherStore != null
				&& this.getClass() == anotherStore.getClass()
				&& ((Store) anotherStore)._id.equals(_id);
	}
	
	/**
	 * Adds all triples (or quads) from the given iterator to the store.
	 * 
	 * @param iterator the statements iterator.
	 * @throws CumulusStoreException if the insertion caused an error.
	 */
	public abstract void addData(Iterator<Statement> iterator) throws CumulusStoreException;

	/**
	 * Adds all triples or quads from the given file to the store.
	 * 
	 * @param file The file to read from.
	 * @param format The format of the file.
	 * @throws CumulusStoreException If an error occurs during adding the data.
	 * @throws IOException If an error occurs during reading from the given file.
	 */
	public void bulkLoad(final File file, final RDFFormat format) throws CumulusStoreException, IOException {
		if (file == null || !file.canRead()) {
			throw new FileNotFoundException(file != null ? file.getAbsolutePath() : "Null input file");
		}
		
		_log.debug(MessageCatalog._00046_BATCH_BULK_LOAD_DATA_STARTS, _batchLimit);

		notifyListeners(_startChangesEvent);

		Reader reader = null;
		final RDFParser rdfParser = Rio.createParser(format);
		final RDFBulkLoadHandler handler = new RDFBulkLoadHandler(_batchLimit);

		try {
			reader = new BufferedReader(new FileReader(file), 8192 * 4);
			rdfParser.setRDFHandler(handler);
			rdfParser.parse(reader, BASE_URI);
		} catch (final RDFParseException exception) {
			_log.debug(MessageCatalog._00029_RDF_PARSE_FAILURE, exception);
			try {
				handler.endRDF();
			} catch (RDFHandlerException ignore) {
				_log.debug(MessageCatalog._00029_RDF_PARSE_FAILURE, ignore);
			}
			throw new CumulusStoreException(exception);
		} catch (final RDFHandlerException exception) {
			_log.debug(MessageCatalog._00029_RDF_PARSE_FAILURE, exception);
			throw new CumulusStoreException(exception);
		} finally {
			
			try {
				handler.endRDF();
			} catch (RDFHandlerException ignore) {
				_log.debug(MessageCatalog._00029_RDF_PARSE_FAILURE, ignore);
			}

			if (reader != null) {
				try {
					reader.close();
				} catch (final Exception ignore) {
					// Ignore
				}
			}
			notifyListeners(_finishedChangesEvent);
		}		
	}
	
	/**
	 * Adds all triples or quads from the file at the given path to the store.<br />
	 * This method uses 2/3 of the connected hosts threads to insert into
	 * Cassandra.
	 * 
	 * @param file The path to the file to read from.
	 * @param format The format of the file.
	 * @throws CumulusStoreException If an error occurs during adding the data.
	 * @throws IOException If an error occurs during reading from the given file.
	 */
	public void bulkLoad(final String file, final RDFFormat format) throws CumulusStoreException, IOException {
		bulkLoad(new File(file), format);
	}

	/**
	 * Adds all triples or quads from the given input stream to the store.
	 * 
	 * @param inputStream The input stream to read from.
	 * @param format The format of the data from the input stream.
	 * @throws CumulusStoreException If an error occurs during adding the data.
	 * @throws IOException If an error occurs during reading from the given file.
	 */
	public void bulkLoad(final InputStream inputStream, final RDFFormat format) throws CumulusStoreException, IOException {
		_log.debug(MessageCatalog._00046_BATCH_BULK_LOAD_DATA_STARTS, _batchLimit);

		notifyListeners(_startChangesEvent);

		final RDFParser rdfParser = Rio.createParser(format);
		rdfParser.setRDFHandler(new RDFBulkLoadHandler(_batchLimit));

		try {
			rdfParser.parse(inputStream, BASE_URI);
		} catch (final RDFParseException exception) {
			_log.debug(MessageCatalog._00029_RDF_PARSE_FAILURE, exception);
			throw new CumulusStoreException(exception);
		} catch (final RDFHandlerException exception) {
			_log.debug(MessageCatalog._00029_RDF_PARSE_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}

		notifyListeners(_finishedChangesEvent);
	}

	/**
	 * Adds the given triple (or quad) to the store.
	 * 
	 * @param triple The triple or quad.
	 * @throws CumulusStoreException If the insertion caused an error.
	 */
	public abstract void addData(Statement triple) throws CumulusStoreException;

	/**
	 * Closes all connections to the underlying storage.
	 */
	public final void close() {
		if (!_isOpen) {
			_log.info(MessageCatalog._00084_STORE_ALREADY_CLOSED, this);
			return;
		}

		if (_selectEstimator != null) {
			_selectEstimator.close();
		}

		if (_counterFactory != null) {
			_counterFactory.close();
		}

		try {
			_rdfIndexDAO.close();
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
		}

		ManagementRegistrar.unregisterStore(
				this, 
				String.valueOf(storageLayout()), 
				getDataAccessLayerFactory().getUnderlyingStorageInfo());

		_workers.shutdown();

		_dictionary.close();
		
		_log.info(MessageCatalog._00085_STORE_HAS_BEEN_CLOSED, this);
		_isOpen = false;
	}

	/**
	 * 
	 * Return a description for the given resource, i.e., the RDF graph
	 * surrounding the resource.
	 * 
	 * @param resource the target resource.
	 * @param include2Hop .
	 * @return a description for the given resource, i.e., the RDF graph surrounding the resource.
	 * @throws CumulusStoreException in case of system internal failure.
	 */
	public Iterator<Statement> describe(final Value resource, final boolean include2Hop) throws CumulusStoreException {
		return describe(resource, include2Hop, Integer.MAX_VALUE, Integer.MAX_VALUE);
	}

	/**
	 * 
	 * Return a description for the given resource, i.e., the RDF graph surrounding the resource.
	 * 
	 * @param resource the target resource.
	 * @param include2Hop .
	 * @param subjects max number of statements in result that contain the given resource as subject.
	 * @param objects max number of statements in result that contain the given resource as object.
	 * @return a description for the given resource, i.e., the RDF graph surrounding the resource.
	 * @throws CumulusStoreException in case of system internal failure.
	 */
	public Iterator<Statement> describe(final Value resource, final boolean include2Hop, final int subjects, final int objects) throws CumulusStoreException {
		return new DescribeIterator(resource, include2Hop, subjects, objects);
	}

	/**
	 * Returns all RDFS classes in this store.
	 * 
	 * @return RDFS classes
	 */
	public abstract Set<Value> getClasses();

	/**
	 * Returns all data-properties in this store.
	 * 
	 * @return data-properties
	 */
	public abstract Set<Value> getDataProperties();

	/**
	 * Returns all object-properties in this store.
	 * 
	 * @return object-properties
	 */
	public abstract Set<Value> getObjectProperties();

	/**
	 * Returns the prefix / namespaces dictionary of this store.
	 * 
	 * @return the prefix / namespaces dictionary of this store.
	 */
	public abstract PersistentMap<String, String> getPrefix2Namespaces();

	/**
	 * Returns the current status of the connected cluster.<br />
	 * That is the status of every pool of the cluster, read/write statistics,
	 * information about the schema and the number of active
	 * additions/deletions.
	 * 
	 * @return The status of the connected cluster.
	 */
	public abstract String getStatus();

	/**
	 * 
	 * Checks if given value v is a RDF class.
	 * 
	 * @param v the value.
	 * @return true if value v is an RDF class.
	 */
	public abstract boolean isClass(Value v);

	/**
	 * 
	 * Checks if given value v is a data-property.
	 * 
	 * @param v the value.
	 * @return true if value v is a data-property.
	 */
	public abstract boolean isDataProperty(Value v);

	/**
	 * 
	 * Checks if given value v is an object-property.
	 * 
	 * @param v the value.
	 * @return true if value v is an object-property.
	 */
	public abstract boolean isObjectProperty(Value v);

	/**
	 * Open the store.
	 * After completing this operation, the store instance is supposed to be available.
	 * 
	 * @throws CumulusStoreException If the connection cannot be established.
	 */
	public final void open() throws CumulusStoreException {
		if (isOpen()) {
			_log.info(MessageCatalog._00049_STORE_ALREADY_OPEN);
			return;
		}

		try {
			ManagementRegistrar.registerStore(
					this, 
					String.valueOf(storageLayout()), 
					getDataAccessLayerFactory().getUnderlyingStorageInfo());
		} catch (InstanceAlreadyExistsException exception) {
			_log.error(MessageCatalog._00111_MBEAN_ALREADY_REGISTERED, getId());
			throw new CumulusStoreException(exception);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00109_UNABLE_TO_REGISTER_MBEAN, exception, getId());
			throw new CumulusStoreException(exception);
		}

		_factory = getDataAccessLayerFactory();
		
		_log.info(MessageCatalog._00114_UNDERLYING_STORAGE, _id, _factory.getUnderlyingStorageInfo());
		
		_configurator.configure(_factory);
		_configurator.configure(this);

		try {
			_dictionary = Dictionaries.newDefaultDictionary(_configurator);
			
			_rdfIndexDAO = getRdfIndexDAO();
			_rdfIndexDAO.initialiseRdfIndex();
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
			throw new CumulusStoreException(exception);
		} 
		
		try {
			
			_dictionary.initialise(_factory);
			_log.info(MessageCatalog._00090_DICTIONARY_INITIALISED, _dictionary.getClass().getName());
		} catch (final InitialisationException exception) {
			_log.error(MessageCatalog._00092_DICTIONARY_INIT_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}

		try {
			final CounterFactory counterFactory = getCounterFactory();
			counterFactory.initialise(_factory);
		} catch (final InitialisationException exception) {
			_log.error(MessageCatalog._00095_COUNTER_FACTORY_INIT_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}
		
		_selectEstimator = new HeuristicsBasedSelectivityEstimator(getCounterFactory());
		_status = new StatusListener();
		_changeListeners = new LinkedList<ITriplesChangesListener>();
		_changeListeners.add(_selectEstimator);
		_changeListeners.add(_status);
		
		openInternal();

		final int howManyWorkers = computeWorkersPoolSize();

		final RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
		_workers = new ThreadPoolExecutor(
				howManyWorkers,
				howManyWorkers,
				0L, TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<Runnable>(100),
				rejectedExecutionHandler);

		_log.info(MessageCatalog._00047_BATCH_BULK_LOAD_DATA_THREAD_POOL_SIZE, howManyWorkers);
		
		_isOpen = true;
		_log.info(MessageCatalog._00052_STORE_OPEN);
	}

	/**
	 * Concrete store instances must specify here their startup and open procedures.
	 * 
	 * @throws CumulusStoreException If the connection cannot be established.
	 */
	protected abstract void openInternal() throws CumulusStoreException;

	/**
	 * Queries the store for triples or quads. A query is an array of 3 or 4
	 * nodes. A null value represents a variable. This method gives an unlimited
	 * amount of results (Integer.MAX_VALUE).
	 * 
	 * @param query
	 *            The query, containing nodes and variables, the array must have
	 *            the form: {s, p, o, c}, c is only used for quad stores.
	 * @return An iterator iterating through the results of the query.
	 * @throws CumulusStoreException
	 *             If an error occurred while executing the query.
	 */
	public abstract Iterator<Statement> query(Value[] query) throws CumulusStoreException;

	/**
	 * Queries the store for triples or quads. A query is an array of 3 or 4
	 * nodes. A null value represents a variable. This amount of results is
	 * limited.
	 * 
	 * @param query
	 *            The query, containing nodes and variables, the array must have
	 *            the form: {s, p, o, c}, c is only used for quad stores.
	 * @param limit
	 *            The maximum amount of results to return.
	 * @return An iterator iterating through the results of the query.
	 * @throws CumulusStoreException
	 *             If an error occurred while executing the query.
	 */
	public abstract Iterator<Statement> query(Value[] query, int limit) throws CumulusStoreException;

	/**
	 * Same as {@link #query(Value[], int)}, but returns node ids instead of
	 * plain nodes.
	 * 
	 * @param query the triple / quad pattern.
	 * @param limit a limit on the total number of results.
	 * @throws CumulusStoreException If an error occurred while executing the query.
	 * @return an iterator containing query result (as identifiers).
	 */
	protected abstract Iterator<byte[][]> queryAsIDs(Value[] query, int limit) throws CumulusStoreException;

	/**
	 * Same as {@link #query(Value[])}, but returns and takes node ids instead of
	 * plain nodes.
	 * 
	 * @param query the triple / quad pattern.
	 * @throws CumulusStoreException If an error occurred while executing the query.
	 * @return an iterator containing query result (as identifiers).
	 */
	public abstract Iterator<byte[][]> queryWithIDs(byte[][] query) throws CumulusStoreException;

	/**
	 * Same as {@link query(Value[], int)}, but returns and takes node ids
	 * instead of plain nodes.
	 * 
	 * @param query the triple / quad pattern.
	 * @param limit a limit on the total number of results.
	 * @throws CumulusStoreException If an error occurred while executing the query.
	 * @return an iterator containing query result (as identifiers).
	 */
	protected abstract Iterator<byte[][]> queryWithIDs(byte[][] query, int limit) throws CumulusStoreException;

	/**
	 * Same as {@link query(Value[])}, but takes node ids instead of plain nodes.
	 * 
	 * @param query the triple / quad pattern.
	 * @throws CumulusStoreException If an error occurred while executing the query.
	 * @return an iterator containing query result (as identifiers).
	 */
	protected abstract Iterator<byte[][]> queryWithIDs(Value[] query) throws CumulusStoreException;

	/**
	 * Performs a range query on the triple store. A range query is a query that
	 * has at least predicate and a range for the object. It returns all triples
	 * with that predicate and a numeric object in the given range.
	 * 
	 * @param query
	 *            The query, must be an array with a length of 2, containing the
	 *            subject (or null if subject is a variable) and the predicate.
	 * @param lowerBound
	 *            The lower bound of the range. Has to be numeric.
	 * @param equalsLower
	 *            True if the lower bound is inclusive (triples having an object
	 *            equal to the lower bound are included), false otherwise.
	 * @param upperBound
	 *            The upper bound of the range. Has to be numeric.
	 * @param equalsUpper
	 *            True if the upper bound is inclusive (triples having an object
	 *            equal to the upper bound are included), false otherwise.
	 * @param reverse
	 *            True if the results should be returned in reverse order.
	 * @param limit
	 *            The maximum amount of results to return.
	 * @return An iterator containing the result of the query.
	 * @throws CumulusStoreException in case of query execution failure.
	 */
	public abstract Iterator<Statement> range(
			Value[] query, 
			Literal lowerBound, 
			boolean equalsLower, 
			Literal upperBound, 
			boolean equalsUpper,
			boolean reverse, 
			int limit) throws CumulusStoreException;

	/**
	 * Same as {@link #range(Value[], Literal, boolean, Literal, boolean, boolean, int)}, but returns node ids instead of nodes.
	 * 
	 * @param query
	 *            The query, must be an array with a length of 2, containing the
	 *            subject (or null if subject is a variable) and the predicate.
	 * @param lowerBound
	 *            The lower bound of the range. Has to be numeric.
	 * @param equalsLower
	 *            True if the lower bound is inclusive (triples having an object
	 *            equal to the lower bound are included), false otherwise.
	 * @param upperBound
	 *            The upper bound of the range. Has to be numeric.
	 * @param equalsUpper
	 *            True if the upper bound is inclusive (triples having an object
	 *            equal to the upper bound are included), false otherwise.
	 * @param reverse
	 *            True if the results should be returned in reverse order.
	 * @param limit
	 *            The maximum amount of results to return.
	 * @return An iterator containing the result (as identifiers) of the query.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public abstract Iterator<byte[][]> rangeAsIDs(
			Value[] query, 
			Literal lowerBound, 
			boolean equalsLower, 
			Literal upperBound, 
			boolean equalsUpper, 
			boolean reverse, 
			int limit) throws DataAccessLayerException;

	/**
	 * Performs a range query on the triple store. A range query is a query that
	 * has at least predicate and a range for the object. It returns all triples
	 * with that predicate and a date/time object in the given range.
	 * 
	 * @param query
	 *            The query, must be an array with a length of 2, containing the
	 *            subject (or null if subject is a variable) and the predicate.
	 * @param lowerBound
	 *            The lower bound of the range. Has to be date/time.
	 * @param equalsLower
	 *            True if the lower bound is inclusive (triples having an object
	 *            equal to the lower bound are included), false otherwise.
	 * @param upperBound
	 *            The upper bound of the range. Has to be date/time.
	 * @param equalsUpper
	 *            True if the upper bound is inclusive (triples having an object
	 *            equal to the upper bound are included), false otherwise.
	 * @param reverse
	 *            True if the results should be returned in reverse order.
	 * @param limit
	 *            The maximum amount of results to return.
	 * @return An iterator containing the result of the query.
	 * @throws CumulusStoreException 
	 */
	public abstract Iterator<Statement> rangeDateTime(
			Value[] query, 
			Literal lowerBound, 
			boolean equalsLower, 
			Literal upperBound,
			boolean equalsUpper, 
			boolean reverse, 
			int limit) throws CumulusStoreException;

	/**
	 * Same as
	 * {@link #rangeDateTime(Value[], Literal, boolean, Literal, boolean, boolean, int)}
	 * , but returns node ids instead of nodes.
	 * @param query
	 *            The query, must be an array with a length of 2, containing the
	 *            subject (or null if subject is a variable) and the predicate.
	 * @param lowerBound
	 *            The lower bound of the range. Has to be numeric.
	 * @param equalsLower
	 *            True if the lower bound is inclusive (triples having an object
	 *            equal to the lower bound are included), false otherwise.
	 * @param upperBound
	 *            The upper bound of the range. Has to be numeric.
	 * @param equalsUpper
	 *            True if the upper bound is inclusive (triples having an object
	 *            equal to the upper bound are included), false otherwise.
	 * @param reverse
	 *            True if the results should be returned in reverse order.
	 * @param limit
	 *            The maximum amount of results to return.
	 * @return An iterator containing the result (as identifiers) of the query.
	 * @throws DataAccessLayerException 
	 */
	public abstract Iterator<byte[][]> rangeDateTimeAsIDs(
			Value[] query, 
			Literal lowerBound, 
			boolean equalsLower, 
			Literal upperBound,
			boolean equalsUpper, 
			boolean reverse, 
			int limit) throws DataAccessLayerException;

	@Override
	public boolean isRangeIndexesSupportEnabled() {
		return _idxRanges;
	}

	/**
	 * Removes all given triples or quads from the store.
	 * 
	 * @param triples An iterator iterating over the triples or quads to delete.
	 * @throws CumulusStoreException If an error occurred during deletion.
	 */
	public abstract void removeData(Iterator<Statement> triples) throws CumulusStoreException;

	/**
	 * Removes the given triple or quad from the store.
	 * 
	 * @param pattern An iterator iterating over the triples or quads to delete.
	 * @throws CumulusStoreException If an error occurred during deletion.
	 */
	public abstract void removeData(Value[] pattern) throws CumulusStoreException;

	/**
	 * Same as {@link #removeData(Iterator)}, but taking node ids instead of
	 * plain nodes.
	 * 
	 * @param pattern An iterator iterating over the triples or quads to delete.
	 * @throws CumulusStoreException If an error occurred during deletion.
	 */
	protected abstract void removeDataWithIDs(byte[][] pattern) throws CumulusStoreException;

	/**
	 * Same as {@link #removeData(Value[])}, but taking node ids instead of plain
	 * nodes.
	 * 
	 * @param triples An iterator iterating over the triples or quads to delete.
	 * @throws CumulusStoreException If an error occurred during deletion.
	 * @throws DataAccessLayerException 
	 */
	protected abstract void removeDataWithIDs(Iterator<byte[][]> triples) throws CumulusStoreException, DataAccessLayerException;

	@Override
	public void setDefaultBatchLimit(final int numOfTriplesPerBatch) {
		if (numOfTriplesPerBatch > 0) {
			_batchLimit = numOfTriplesPerBatch;
		}
	}

	@Override
	public int getDefaultBatchLimit() {
		return _batchLimit;
	}

	@Override
	public boolean activeChanges() {
		return _status.active();
	}

	/**
	 * Enables the range indexes on this store instance.
	 * Note that this must be called *before* opening the store.
	 */
	void enableRangeIndexesSupport() {
		if (!isOpen()) {
			_idxRanges = true;
		}
	}
	
	/**
	 * Clears all data within this store.
	 */
	public void clear() {
		_rdfIndexDAO.clear();
	}

	/**
	 * Returns the layout of this store.
	 * 
	 * @return the layout of this store.
	 */
	protected abstract StorageLayout storageLayout();

	/**
	 * Returns the data access layer factory associated with this store.
	 * 
	 * @return the data access layer factory associated with this store.
	 */
	protected DataAccessLayerFactory getDataAccessLayerFactory() {
		return DataAccessLayerFactory.getDefaultDataAccessLayerFactory(storageLayout());
	}

	/**
	 * Fires a given event object to registered listeners.
	 * 
	 * @param event the event object.
	 */
	void notifyListeners(final EventObject event) {
		for (final ITriplesChangesListener listener : _changeListeners) {
			listener.update(event);
		}
	}

	/**
	 * Inserts the given triples/quads into the store.
	 * 
	 * @param nodes an iterator iterating over the triples/quads to insert.
	 * @param batchSize the maximum size of a batch query.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void batchInsert(final Iterator<Statement> nodes, final int batchSize) throws DataAccessLayerException {

		final List<byte[][]> triples = new ArrayList<byte[][]>(Math.min(1000, batchSize));

		while (nodes.hasNext()) {
			for (int i = 0; i < batchSize && nodes.hasNext(); i++) {

				byte[][] ids;
				final Statement statement = nodes.next();

				ids = (statement.getContext() != null)
						? _dictionary.getIDs(
								statement.getSubject(),
								statement.getPredicate(),
								statement.getObject(),
								statement.getContext())
						: _dictionary.getIDs(
								statement.getSubject(),
								statement.getPredicate(),
								statement.getObject());

				_rdfIndexDAO.insertTriple(ids);

				if (_idxRanges && statement.getObject() instanceof Literal) {
					final Literal literal = (Literal) statement.getObject();
					if (NUMERIC_RANGETYPES.contains(literal.getDatatype())) {
						try {

							double value = Double.parseDouble(literal.getLabel());
							_rdfIndexDAO.insertRanges(ids, value);
						} catch (final NumberFormatException exception) {
							_log.error(MessageCatalog._00059_BAD_DOUBLE_VALUE, exception, literal);
						}
					} else if (DATETIME_RANGETYPES.contains(literal.getDatatype())) {
						try {

							long value = Util.parseXMLSchemaDateTimeAsMSecs(literal);
							_rdfIndexDAO.insertRanges(ids, value);
						} catch (final NumberFormatException exception) {
							_log.error(MessageCatalog._00060_BAD_LITERAL_VALUE, exception, literal);
						}
					}
				}
				triples.add(ids);
			}

			_rdfIndexDAO.executePendingMutations();
		}

		notifyListeners(new AddTripleEvent(DUMMY_SOURCE_EVENT, triples));
	}

	/**
	 * Returns an RDF index DAO for this (triple) store.
	 * 
	 * @return an RDF index DAO for this store.
	 */
	abstract TripleIndexDAO getRdfIndexDAO();
	
	/**
	 * Computes the max number of workers (i.e. threads) for bulk operations.
	 * 
	 * TODO: 3 hosts have been hardcoded because that number comes from a C* specific class (Cluster)
	 * @return the max number of workers (i.e. threads) for bulk operations.
	 */
	protected int computeWorkersPoolSize() {
		return Runtime.getRuntime().availableProcessors()
				+ (int) Math.max(1, (3 / 1.5));
	}	
	
	@Override
	public long triplesCount() {
		return _selectEstimator.triplesCount();
	}	
}