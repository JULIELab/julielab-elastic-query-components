package de.julielab.elastic.query.components.data.aggregation;

import java.util.HashMap;
import java.util.Map;

import de.julielab.elastic.query.components.data.SearchServerCommand;

/**
 * <p>
 * This class is meant as a part of {@link SearchServerCommand}. It expresses data aggregation commands for the search
 * server. In some cases, aggregations can be seen as a 'group by'-like feature, however the approach is more general.
 * For example, the 'maximum' aggregation only returns the maximum value of a field (or even the document score), the
 * 'terms' aggregation is basically the term facets approach. This is heavily modeled after ElasticSearch aggregations.
 * <p>
 * <p>
 * Aggregations can be nested, so this class serves as a common super type for aggregation commands to allow flexible,
 * general nesting.
 * </p>
 * 
 * @see http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations.html
 * 
 * @author faessler
 * 
 */
public abstract class AggregationCommand {
	/**
	 * The name of this aggregation to identify it in the results.
	 */
	public String name;
	/**
	 * A list of sub-aggregations of this aggregation. May be <tt>null</tt>.
	 */
	public Map<String, AggregationCommand> subaggregations;

	public void addSubaggregation(AggregationCommand aggregation) {
		if (null == subaggregations)
			subaggregations = new HashMap<>();
		subaggregations.put(aggregation.name, aggregation);
	}

	public AggregationCommand getSubaggregation(String name) {
		if (null == subaggregations || null == subaggregations.get(name))
			return null;
		return subaggregations.get(name);
	}

	public static class OrderCommand {
		/**
		 * Enumeration constants to determine whether to sort ascending or descending by <tt>reference</tt>.
		 * 
		 * @author faessler
		 * 
		 */
		public enum SortOrder {
			DESCENDING, ASCENDING
		}

		public enum ReferenceType {
			AGGREGATION_SINGLE_VALUE,
			/**
			 * Requires {@link OrderCommand#metric} to be set in order to obtain a single value to order by.
			 */
			AGGREGATION_MULTIVALUE, COUNT, TERM
		}

		public enum Metric {
			avg, min, max
		}

		/**
		 * The way of ordering. May just be the count of each term, its name or even another value given by a subaggregation.
		 * @see ReferenceType
		 */
		public ReferenceType referenceType;
		/**
		 * A reference to another aggregation to use its values to sort this aggregation by. The reference is identified
		 * by this subaggregation's name.
		 */
		public String referenceName;
		/**
		 * Whether to sort descending or ascending by the value of {@link #referenceName}.
		 */
		public SortOrder sortOrder;
		/**
		 * Only used for order type {@link ReferenceType#AGGREGATION_MULTIVALUE}. This metric is used to receive one
		 * single value from a multi-valued aggregation referenced by {@link #referenceName}.
		 */
		public Metric metric;
	}
}
