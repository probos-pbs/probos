package uk.ac.gla.terrier.probos.common;

import java.util.Map;

public class MapEntry<K,V> implements Map.Entry<K, V>
{
	K _key;
	V _val;
	
	public MapEntry(K k, V v) {
		this._key = k;
		this._val = v;
	}
	
	@Override
	public K getKey() {
		return _key;
	}

	@Override
	public V getValue() {
		return _val;
	}

	@Override
	public V setValue(V value) {
		V old = _val;
		_val = value;
		return old;
	}
	
}