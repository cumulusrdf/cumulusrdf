package edu.kit.aifb.cumulus.store.dict.impl.value;

import static edu.kit.aifb.cumulus.framework.util.Bytes.concat;
import static edu.kit.aifb.cumulus.framework.util.Bytes.subarray;
import static edu.kit.aifb.cumulus.store.sesame.CumulusRDFSesameUtil.SESAME_VALUE_FACTORY;

import org.openrdf.model.URI;
import org.openrdf.model.Value;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.domain.dictionary.IDictionary;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * A dictionary with a good compromise between I/O and in-memory work.
 * Basically, values are managed differently depending on their nature:
 * 
 * <ul>
 * 	<li>Namespaces and local names are managed using two different (String) dictionaries;</li>
 * 	<li>Blank nodes and literals are managed using a dedicated {@link ITopLevelDictionary}.</li>
 * </ul>
 *  
 * TODO: At the moment, persistent string dictionaries are supposed to create fixed-length identifiers. 
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class ThreeTieredValueDictionary extends ValueDictionaryBase {
	static final byte MARKER = 30;

	private final IDictionary<String> _namespaces;
	private final IDictionary<String> _localNames;

	private final ITopLevelDictionary _bNodesAndLiterals;
	
	/**
	 * Builds a new dictionary with given (sub)dictionaries.
	 * 
	 * @param id the dictionary identifier.
	 * @param namespaces the dictionary that will be used for namespaces.
	 * @param localNames the dictionary that will be used for local names.
	 * @param bNodesAndLiterals the dictionary that will be used for local names and other kind of resources.
	 */
	public ThreeTieredValueDictionary(
			final String id,
			final IDictionary<String> namespaces,
			final IDictionary<String> localNames,
			final ITopLevelDictionary bNodesAndLiterals) {
		super(id);
		
		if (namespaces == null || localNames == null || bNodesAndLiterals == null) {
			throw new IllegalArgumentException(MessageCatalog._00091_NULL_DECORATEE_DICT);
		}
		
		_namespaces = namespaces;
		_localNames = localNames;
		_bNodesAndLiterals = bNodesAndLiterals;
	}
	
	@Override
	protected void initialiseInternal(final DataAccessLayerFactory factory) throws InitialisationException {		
		_namespaces.initialise(factory);
		_localNames.initialise(factory);
		_bNodesAndLiterals.initialise(factory);
	}

	@Override
	protected byte[] getIdInternal(final Value value, final boolean p) throws DataAccessLayerException {
		if (value instanceof URI) {
			final URI uri = (URI) value;
			byte[] namespaceId = _namespaces.getID(uri.getNamespace(), p);
			byte[] localNameId = _localNames.getID(uri.getLocalName(), p);
			return concat(MARKER, namespaceId, localNameId);
		} else {
			return _bNodesAndLiterals.getID(value, p);
		}
	}

	@Override
	protected Value getValueInternal(final byte[] id, final boolean p) throws DataAccessLayerException {
		if (id[0] == MARKER) {
			return SESAME_VALUE_FACTORY.createURI(
					_namespaces.getValue(subarray(id, 1, 8), p),
					_localNames.getValue(subarray(id, 9, id.length - 9), p));
		} else {
			return _bNodesAndLiterals.getValue(id, p);
		}
	}

	@Override
	public void removeValue(final Value value, final boolean p) throws DataAccessLayerException {
		if (value != null && !(value instanceof URI)) {
			_bNodesAndLiterals.removeValue(value, p);
		}
	}

	@Override
	protected void closeInternal() {
		_namespaces.close();
		_localNames.close();
		_bNodesAndLiterals.close();
	}

	@Override
	public boolean isBNode(final byte[] id) {
		return id != null && id[0] != MARKER && _bNodesAndLiterals.isBNode(id);
	}

	@Override
	public boolean isLiteral(final byte[] id) {
		return id != null && id[0] != MARKER && _bNodesAndLiterals.isLiteral(id);
	}

	@Override
	public boolean isResource(final byte[] id) {
		return id != null && id[0] == MARKER;
	}
}