package edu.kit.aifb.cumulus.store.dict.impl.value;

import static edu.kit.aifb.cumulus.framework.Environment.CHARSET_UTF8;
import static edu.kit.aifb.cumulus.store.sesame.CumulusRDFSesameUtil.SESAME_VALUE_FACTORY;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.DC;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.DOAP;
import org.openrdf.model.vocabulary.EARL;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.SKOS;
import org.openrdf.rio.ntriples.NTriplesUtil;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.framework.util.Utility;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.mx.ManageableKnownURIsDictionary;

/**
 * A dictionary that manages a fixed set of vocabularies.
 * This is useful when you want to separate the management of triples coming 
 * from well-known vocabularies such dc, dcterms, foaf.
 * Enabling this dictionary, which is not supposed to be used standalone, and 
 * decorating it with a {@link CacheValueDictionary} having a size moreless equal to 
 * the expected number of triples in managed vocabularies, allows for fast (in memory)
 * lookup of the corresponding entries.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class KnownURIsDictionary extends SingleIndexValueDictionary implements ManageableKnownURIsDictionary {

	static final byte KNOWN_URI_MARKER = 31;
	static final int ID_LENGTH = 19;
	static final String [] DEFAULT_DOMAINS = {
		DC.NAMESPACE,
		DCTERMS.NAMESPACE,
		DOAP.NAMESPACE,
		EARL.NAMESPACE,
		FOAF.NAMESPACE,
		OWL.NAMESPACE,
		RDF.NAMESPACE,
		RDFS.NAMESPACE,
		SKOS.NAMESPACE};
	
	private ITopLevelDictionary _decoratee;
	final String[] _domains;

	private final AtomicLong _idKnownURIsHitsCount = new AtomicLong();
	private final AtomicLong _valueknownURIsHitsCount = new AtomicLong();
	
	/**
	 * Builds a new known uris dictionary.
	 * 
	 * @param id the dictionary identifier.
	 * @param decoratee the decorated dictionary.
	 */
	public KnownURIsDictionary(final String id, final ITopLevelDictionary decoratee) {
		this(id, decoratee, (String[])null);
	}

	/**
	 * Builds a new known uris dictionary.
	 * 
	 * @param id the dictionary identifier.
	 * @param decoratee the decorated dictionary.
	 * @param domains the domains that will be managed by this dictionary.
	 */
	public KnownURIsDictionary(final String id, final ITopLevelDictionary decoratee, final String ... domains) {
		super(id, "DICT_WELL_KNOWN_URIS");
		
		if (decoratee == null) {
			throw new IllegalArgumentException(MessageCatalog._00091_NULL_DECORATEE_DICT);
		}
		
		_decoratee = decoratee;
		_domains = (domains != null && domains.length > 0) ? domains : DEFAULT_DOMAINS;
	}

	@Override
	public void initialiseInternal(final DataAccessLayerFactory factory) throws InitialisationException {		
		super.initialiseInternal(factory);
		_decoratee.initialise(factory);
	}

	@Override
	protected byte[] getIdInternal(final Value value, final boolean p) throws DataAccessLayerException {
		if (value instanceof URI && contains(((URI) value).getNamespace())) {
			RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
			_idKnownURIsHitsCount.incrementAndGet();
			final String n3 = NTriplesUtil.toNTriplesString(value);
			byte[] id = null;

			synchronized (this) {
				id = getID(NTriplesUtil.toNTriplesString(value), p);
				if (id[0] == NOT_SET[0]) {
					id = newId(n3, _index);
					_index.putQuick(n3, id);
				}
			}
			return id;
		} else {
			RUNTIME_CONTEXTS.get().isFirstLevelResult = false;
			return _decoratee.getID(value, p);
		}
	}

	@Override
	protected Value getValueInternal(final byte[] id, final boolean p) throws DataAccessLayerException {
		if (id[0] == KNOWN_URI_MARKER && id.length == ID_LENGTH) {
			RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
			_valueknownURIsHitsCount.incrementAndGet();
			return NTriplesUtil.parseResource(getN3(id, p), SESAME_VALUE_FACTORY);
		} else {
			RUNTIME_CONTEXTS.get().isFirstLevelResult = false;
			return _decoratee.getValue(id, p);
		}
	}

	@Override
	public void removeValue(final Value value, final boolean p) throws DataAccessLayerException {
		if (value instanceof URI && contains(((URI) value).getNamespace())) {
			final String n3 = NTriplesUtil.toNTriplesString(value);
			_index.remove(n3);
		} else {
			_decoratee.removeValue(value, p);
		}
	}

	/**
	 * Checks if the given prefix is managed by this dictionary.
	 * 
	 * @param prefix the prefix to check.
	 * @return true if the given prefix is managed by this dictionary.
	 */
	boolean contains(final String prefix) {
		for (final String domain : _domains) {
			if (domain.equals(prefix)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Creates a new (hash) identifier for the given resource.
	 * 
	 * @param n3 the N3 representation of the resource.
	 * @return a new (hash) identifier for the given resource.
	 */
	protected byte[] makeNewHashID(final String n3) {
		final byte[] hash = Utility.murmurHash3(n3.getBytes(CHARSET_UTF8)).asBytes();
		final ByteBuffer buffer = ByteBuffer.allocate(ID_LENGTH);
		buffer.put(KNOWN_URI_MARKER);
		buffer.put(RESOURCE_BYTE_FLAG);

		buffer.put(hash);
		buffer.flip();
		return buffer.array();
	}

	@Override
	public boolean isBNode(final byte[] id) {
		return id != null && id[0] != KNOWN_URI_MARKER && _decoratee.isBNode(id);
	}

	@Override
	public boolean isLiteral(final byte[] id) {
		return id != null && id[0] != KNOWN_URI_MARKER && _decoratee.isLiteral(id);
	}

	@Override
	public boolean isResource(final byte[] id) {
		return id != null && ((id[0] == KNOWN_URI_MARKER && id[1] == RESOURCE_BYTE_FLAG && id.length == ID_LENGTH)
				|| (_decoratee.isResource(id)));
	}

	@Override
	public long getIdKnownURIsHitsCount() {
		return _idKnownURIsHitsCount.get();
	}

	@Override
	public long getValueKnownURIsHitsCount() {
		return _valueknownURIsHitsCount.get();
	}
	
	@Override
	protected void closeInternal() {
		_decoratee.close();
	}	
}