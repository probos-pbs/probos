/**
 * Copyright (c) 2016, University of Glasgow. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

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