package edu.kit.aifb.cumulus.store;

import java.util.Arrays;
import java.util.EventObject;
import java.util.Iterator;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.Initialisable;
import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.framework.events.ITriplesChangesListener;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.dict.impl.value.ValueDictionaryBase;
import edu.kit.aifb.cumulus.store.events.AddTripleEvent;
import edu.kit.aifb.cumulus.store.events.RemoveTriplesEvent;

/**
 * CumulusRDF schema.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Schema implements ITriplesChangesListener, Initialisable {

	private static final Log LOG = new Log(LoggerFactory.getLogger(Schema.class));
	private static final String COL_CLAZZES = "SCHEMA_CLASSES", COL_D_PROPS = "SCHEMA_D_PROPS", COL_O_PROPS = "SCHEMA_O_PROPS";

	private final PersistentSet<byte[]> _classesSet, _dPropsSet, _oPropsSet;
	private final DictionarySetDecorator _classes, _dataProperties, _objectProperties;
	private final ITopLevelDictionary _dictionary;
	
	public void index(final Store store, final ITopLevelDictionary dictionary) throws DataAccessLayerException {

		PersistentSet<byte[]> clazzes_cassandra = new PersistentSet<byte[]>(byte[].class, COL_CLAZZES);
		PersistentSet<byte[]> d_props_cassandra = new PersistentSet<byte[]>(byte[].class, COL_D_PROPS);
		PersistentSet<byte[]> o_props_cassandra = new PersistentSet<byte[]>(byte[].class, COL_O_PROPS);

		try {

			long counter = 0;

			LOG.info(MessageCatalog._00063_CREATING_NEW_SCHEMA);

			for (final Iterator<Statement> iterator = store.query(new Value[] {null, RDF.TYPE, null}); iterator.hasNext();) {

				// TODO it should be sufficient to just "getID" here
				clazzes_cassandra.add(dictionary.getID(iterator.next().getObject(), false));

				if (++counter % 1000 == 0) {
					LOG.info(MessageCatalog._00064_HOW_MANY_TYPE_TRIPLES, counter);
				}
			}

			LOG.info(MessageCatalog._00065_HOW_MANY_CLASSES_FOUND, clazzes_cassandra.size());
			counter = 0;

			/*
			 * probe each class for instances ...
			 */
			for (byte[] clazz_id : clazzes_cassandra) {

				final Value clazz = dictionary.getValue(clazz_id, false);
				LOG.info(MessageCatalog._00066_SAMPLING_OVER, clazz);

				for (Iterator<Statement> clazz_iter = store.query(new Value[] {null, RDF.TYPE, clazz}); clazz_iter.hasNext();) {

					for (final Iterator<Statement> instance_iter = store.query(
							new Value[] {clazz_iter.next().getSubject(), null, null}, 100); instance_iter.hasNext();) {

						final Statement triple = instance_iter.next();
						final Value object = triple.getObject();
						final URI predicate = triple.getPredicate();
						
						if (object instanceof Literal) {
							// TODO it should be sufficient to just "getID" here
							d_props_cassandra.add(dictionary.getID(predicate, true));
						} else {
							// TODO it should be sufficient to just "getID" here
							o_props_cassandra.add(dictionary.getID(predicate, true));
						}

						if (++counter % 1000 == 0) {
							LOG.info(MessageCatalog._00067_HOW_MANY_INSTANCE_TRIPLES, counter);
						}
					}
				}
			}

			LOG.info(MessageCatalog._00068_SAMPLING_COMPLETED, counter);
		} catch (final CumulusStoreException exception) {
			LOG.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE, exception);
		}
	}
	
	/**
	 * Builds a new schema for a given store.
	 * 
	 * @param dictionary the dictionary used by the store.
	 */
	protected Schema(final ITopLevelDictionary dictionary) {
		_dictionary = dictionary;

		_classesSet = new PersistentSet<byte[]>(byte[].class, COL_CLAZZES);
		_dPropsSet = new PersistentSet<byte[]>(byte[].class, COL_D_PROPS);
		_oPropsSet = new PersistentSet<byte[]>(byte[].class, COL_O_PROPS);

		_classes = new DictionarySetDecorator(_classesSet, _dictionary, false);
		_dataProperties = new DictionarySetDecorator(_dPropsSet, _dictionary, true);
		_objectProperties = new DictionarySetDecorator(_oPropsSet, _dictionary, true);
	}

	@Override
	public void initialise(final DataAccessLayerFactory factory) throws InitialisationException {
		_classesSet.initialise(factory);
		_dPropsSet.initialise(factory);
		_oPropsSet.initialise(factory);
	}
	
	/**
	 * Returns classes managed by this schema.
	 * 
	 * @return classes in the data store.
	 */
	protected Set<Value> getClasses() {
		return _classes;
	}

	/**
	 * Returns datatype properties in the data store.
	 * 
	 * @return datatype properties in the data store
	 */
	protected Set<Value> getDatatypeProperties() {
		return _dataProperties;
	}

	/**
	 * Not yet implemented.
	 * 
	 * @param prop the resource. 
	 * @return domain of a given property.
	 */
	protected Set<Value> getDomain(final Resource prop) {
		// TODO
		return null;
	}

	/**
	 * Returns object properties in the data store.
	 * 
	 * @return object properties in the data store
	 */
	protected Set<Value> getObjectProperties() {
		return _objectProperties;
	}

	/**
	 * Not yet implemented.
	 * 
	 * @param prop
	 * @return Range of a given property.
	 */
	protected Set<Value> getRange(final Value prop) {
		// TODO
		return null;
	}

	/**
	 * Not yet implemented.
	 * 
	 * @param clazz
	 * @return Subclasses of a class.
	 */
	protected Set<Value> getSubClasses(final Value clazz) {
		// TODO
		return null;
	}

	/**
	 * Not yet implemented.
	 * 
	 * @return Subproperties of a property.
	 */
	protected Set<Value> getSubProperties(final Value prop) {
		// TODO
		return null;
	}

	/**
	 * Not yet implemented.
	 * 
	 * @return Superclasses of a class.
	 */
	protected Set<Value> getSuperClasses(final Value clazz) {
		// TODO
		return null;
	}

	/**
	 * Not yet implemented.
	 * 
	 * @return Superproperties of a property.
	 */
	protected Set<Value> getSuperProperties(final Value prop) {
		// TODO
		return null;
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append("Class: ").append(getClass().getName()).append(", ")
				.append("Classes: ").append(_classes).append(", ")
				.append("Datatype properties: ").append(_dataProperties).append(", ")
				.append("Object properties:").append(_objectProperties)
				.toString();
	}

	@Override
	public void update(final EventObject event) {
		try {
			if (event instanceof AddTripleEvent) {
	
				AddTripleEvent add_event = (AddTripleEvent) event;
				// TODO: index subclass + subproperty hiearchies??
	
				for (byte[][] triple : add_event.getChangedTriples()) {
	
					if (triple == null || triple.length < 3) {
						continue;
					}
	
					try {
	
						// update clazzes
						if (Arrays.equals(triple[1], _dictionary.getID(RDF.TYPE, true))) {
	
							if (!_classesSet.contains(triple[2])) {
								_classesSet.add(triple[2]);
							}
						}
						// update clazzes
						else if (Arrays.equals(triple[1], _dictionary.getID(RDFS.SUBCLASSOF, true))) {
	
							if (!_classesSet.contains(triple[0])) {
								_classesSet.add(triple[0]);
							}
	
							if (!_classesSet.contains(triple[2])) {
								_classesSet.add(triple[2]);
							}
						}
						// update object propeties
						else if (triple[2][0] == ValueDictionaryBase.RESOURCE_BYTE_FLAG || triple[2][0] == ValueDictionaryBase.BNODE_BYTE_FLAG) {
	
							if (!_oPropsSet.contains(triple[1])) {
								_oPropsSet.add(triple[1]);
							}
						}
						// update data propeties
						else if (triple[2][0] == ValueDictionaryBase.LITERAL_BYTE_FLAG) {
	
							if (!_dPropsSet.contains(triple[1])) {
								_dPropsSet.add(triple[1]);
							}
						}
	
					} catch (IllegalArgumentException e) {
						LOG.error("could not get node for triple: " + Arrays.toString(triple), e);
					}
				}
			} else if (event instanceof RemoveTriplesEvent) {
				// TODO: remove schema elements if necessary
			}
		} catch (final DataAccessLayerException exception) {
			LOG.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
		}
	}
}