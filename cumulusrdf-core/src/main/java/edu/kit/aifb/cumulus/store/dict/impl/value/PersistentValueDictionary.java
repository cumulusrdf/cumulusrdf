package edu.kit.aifb.cumulus.store.dict.impl.value;

import static edu.kit.aifb.cumulus.framework.Environment.CHARSET_UTF8;
import static edu.kit.aifb.cumulus.framework.util.Bytes.fillIn;
import static edu.kit.aifb.cumulus.framework.util.Bytes.subarray;
import static edu.kit.aifb.cumulus.store.sesame.CumulusRDFSesameUtil.SESAME_VALUE_FACTORY;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.rio.ntriples.NTriplesUtil;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.util.Utility;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.BIndex;

/**
 * Simple implementation of a node dictionary. 
 * Uses MurmurHash3 hashing and linear probing for hash collision resolution.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
// FIXME deal with node deletion ...
public class PersistentValueDictionary extends ValueDictionaryBase {
	static final int ID_LENGTH = 17;

	private BIndex _soIndex;
	private BIndex _pIndex;

	/**
	 * Builds a new dictionary.
	 * 
	 * @param id the dictionary identifier.
	 */
	public PersistentValueDictionary(final String id) {
		super(id);
	} 
	
	@Override
	public void initialiseInternal(final DataAccessLayerFactory factory) throws InitialisationException {		
		_soIndex = new BIndex("DICT_SO");
		_soIndex.initialise(factory);
		
		_pIndex = new BIndex("DICT_P");	
		_pIndex.initialise(factory);
	}

	@Override
	public void closeInternal() {
		// Nothing to be done here...
	}
	
	@Override
	protected Value getValueInternal(final byte[] id, final boolean p) throws DataAccessLayerException {
		final String n3 = getN3(id, p);
		if (id[0] == RESOURCE_BYTE_FLAG) {
			return NTriplesUtil.parseResource(n3, SESAME_VALUE_FACTORY);
		} else if (id[0] == LITERAL_BYTE_FLAG) {
			return NTriplesUtil.parseLiteral(n3, SESAME_VALUE_FACTORY);
		} else {
			return NTriplesUtil.parseBNode(n3, SESAME_VALUE_FACTORY);
		}
	}

	/**
	 * Returns the identifier of a given N3 resource.
	 * 
	 * @param n3 the resource (N3 representation).
	 * @param p a flag indicating if the resource is a predicate.
	 * @return the identifier of the given resource.
	 * @throws DataAccessLayerException in case of data access failure. 
	 */
	protected byte[] getID(final String n3, final boolean p) throws DataAccessLayerException {
		return (n3 == null || n3.isEmpty() || n3.charAt(0) == '?')
				? null
				: p ? _pIndex.get(n3) : _soIndex.get(n3);
	}

	@Override
	protected byte[] getIdInternal(final Value value, final boolean p) throws DataAccessLayerException {		
		final String n3 = NTriplesUtil.toNTriplesString(value);
		byte[] id = null;

		synchronized (this) {
			
			id = value != null ? getID(NTriplesUtil.toNTriplesString(value), p) : null;

			if (id[0] == NOT_SET[0]) {
				final BIndex index = p ? _pIndex : _soIndex;
				id = newId(value, n3, index);
				index.putQuick(n3, id);
			}
		}
		return id;
	}

	/**
	 * Creates a new identifier for a given resource.
	 * The method takes care about (eventual) hash collision.
	 * 
	 * @param value the resource. 
	 * @param n3 the N3 representation of resource.
	 * @param index the dictionary index that could already hold that resource / id.
	 * @return a new identifier for the given resource.
	 * @throws DataAccessLayerException in case of data access failure. 
	 */
	private byte[] newId(final Value value, final String n3, final BIndex index) throws DataAccessLayerException {
		byte[] id = makeNewHashID(value, n3);
		for (int i = 0; index.contains(id) && i <= 100; i++) {

			id = resolveHashCollision(id, i);

			if (i == 100) {
				_log.error(MessageCatalog._00071_UNABLE_TO_RESOLVE_COLLISION, n3, i);
			}
		}
		return id;
	}

	/**
	 * Creates a new (hash) identifier for the given resource.
	 * 
	 * @param node the resource.
	 * @param n3 the N3 representation of the resource.
	 * @return a new (hash) identifier for the given resource.
	 */
	private static byte[] makeNewHashID(final Value node, final String n3) {

		final byte[] hash = Utility.murmurHash3(n3.getBytes(CHARSET_UTF8)).asBytes();

		final ByteBuffer buffer = ByteBuffer.allocate(ID_LENGTH);

		if (node instanceof Literal) {
			buffer.put(LITERAL_BYTE_FLAG);
		} else if (node instanceof BNode) {
			buffer.put(BNODE_BYTE_FLAG);
		} else {
			buffer.put(RESOURCE_BYTE_FLAG);
		}

		buffer.put(hash);
		buffer.flip();
		return buffer.array();
	}

	@Override
	public void removeValue(final Value value, final boolean p) throws DataAccessLayerException {

		if (value != null) {

			final String n3 = NTriplesUtil.toNTriplesString(value);

			if (p) {
				_pIndex.remove(n3);
			} else {
				_soIndex.remove(n3);
			}
		}
	}

	/**
	 * Resolves hash collision.
	 * 
	 * @param id the computed (hash) identifier.
	 * @param step the number of step to use in algorithm.
	 * @return the resolved hash identifier.
	 */
	private byte[] resolveHashCollision(final byte[] id, final int step) {

		final ByteBuffer buffer = ByteBuffer.wrap(id);
		long hash = buffer.getLong(1);
		// linear probing
		buffer.putLong(1, ++hash).flip();
		return buffer.array();
	}

	@Override
	public byte[][] decompose(final byte[] compositeId) {
		if (compositeId != null && compositeId.length > 0) {
			final int howManyValues = compositeId.length / ID_LENGTH;
			final byte[][] tuple = new byte[howManyValues][];
			for (int i = 0; i < howManyValues; i++) {
				tuple[i] = subarray(compositeId, i * ID_LENGTH, ID_LENGTH);
			}
			return tuple;
		}
		return null;
	}

	@Override
	public byte[] compose(final byte[] id1, final byte [] id2) {
		byte [] result = new byte [id1.length + id2.length];
		fillIn(result, 0, id1);
		fillIn(result, id1.length, id2);
		return result;
	}

	@Override
	public byte[] compose(final byte[] id1, final byte [] id2, final byte[] id3) {
		byte [] result = new byte [id1.length + id2.length + id3.length];
		fillIn(result, 0, id1);
		fillIn(result, id1.length, id2);		
		fillIn(result, id1.length + id2.length, id3);		
		return result;
	}

	@Override
	public boolean isBNode(final byte[] id) {
		return id != null && id.length == ID_LENGTH && id[0] == BNODE_BYTE_FLAG;
	}

	@Override
	public boolean isLiteral(final byte[] id) {
		return id != null && id.length == ID_LENGTH && id[0] == LITERAL_BYTE_FLAG;
	}

	@Override
	public boolean isResource(final byte[] id) {
		return id != null && id.length == ID_LENGTH && id[0] == RESOURCE_BYTE_FLAG;
	}

	/**
	 * Returns the N3 representation of the value associated with a given identifier.
	 * 
	 * @param id the value identifier.
	 * @param p the predicate flag.
	 * @return the N3 representation of the value associated with a given identifier.
	 * @throws DataAccessLayerException in case of data access failure. 
	 */
	String getN3(final byte[] id, final boolean p) throws DataAccessLayerException {
		if (id == null || id.length == 0) {
			return null;
		}

		final String n3 = p ? _pIndex.getQuick(id) : _soIndex.getQuick(id);
		if (n3 == null || n3.isEmpty()) {
			_log.error(MessageCatalog._00086_NODE_NOT_FOUND_IN_DICTIONARY, Arrays.toString(id));
		}

		return n3;
	}
}