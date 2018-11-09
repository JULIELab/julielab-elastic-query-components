package de.julielab.elastic.query.components.data.aggregation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The terms aggregation is basically faceting on term level.
 * 
 * @see <a href="http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html">http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html</a>
 * @author faessler
 * 
 */
public class TermsAggregation extends AggregationRequest {
	/**
	 * The field from which to retrieve terms to aggregate over.
	 */
	public String field;
	/**
	 * Specifies the ordering of the aggregation buckets. Defaults to bucket
	 * size.
	 */
	public List<OrderCommand> order;
	/**
	 * Either a string that will be interpreted as a regular expression matching
	 * terms to include or an instance of {@link Collection} or an array containing field values (of type String, Long or Double)
	 * exactly enumerating the included items.
	 */
	public Object include;
	/**
	 * Either a string that will be interpreted as a regular expression matching
	 * terms to exclude or an instance of {@link Collection} or an array containing field values (of type String, Long or Double)
	 * exactly enumerating the excluded items.
	 */
	public Object exclude;
	/**
	 * The number of top-terms to be used for aggregation.
	 */
	public Integer size;

    @Override
    public TermsAggregation clone() throws CloneNotSupportedException {
        TermsAggregation clone = (TermsAggregation) super.clone();
        clone.order = order.stream().collect(Collectors.toList());
        clone.include = include instanceof String ? include : ((Collection) include).stream().collect(Collectors.toList());
        clone.exclude = exclude instanceof String ? exclude : ((Collection) exclude).stream().collect(Collectors.toList());
        return clone;
    }

    public void addOrder(OrderCommand orderItem) {
		if (null == order)
			order = new ArrayList<>();
		order.add(orderItem);
	}
}
