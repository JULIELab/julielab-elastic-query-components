package de.julielab.elastic.query.components.data;

import java.util.HashMap;
import java.util.Map;

public class FieldTermItem {

	/**
	 * The different types of aggregation values that can be associated with an index field term. This includes its
	 * frequency in a particular query or the maximum score of a document this term occurred in.
	 * 
	 * @author faessler
	 * 
	 */
	public enum ValueType {
		/**
		 * The term count is encoded as a {@link Long} in {@link #values()}, if present.
		 */
		COUNT,
		/**
		 * The maximum document score for this term item is encoded as a {@link Double} in {@link #values()}, if
		 * present.
		 */
		MAX_DOC_SCORE
	}

	/**
	 * Returns the value for the term at the current position of the stream.
	 * 
	 * @param valueType
	 *            The type of value to be returned.
	 * @return The value for the term at the current position of the stream.
	 * @see {@link ValueType}
	 */
	public Map<ValueType, Object> values = new HashMap<>();
	/**
	 * The term itself, i.e. the raw object in the Lucene index field.
	 */

	public Object term;

	/**
	 * Adds a <tt>value</tt> for the {@link ValueType} at the end of the stream.
	 * 
	 * @param valueType
	 * @param value
	 */
	public void setValue(ValueType valueType, Object value) {
		values.put(valueType, value);
	}

	@Override
	public String toString() {
		return "FieldTermItem [values=" + values + ", term=" + term + "]";
	}

}
