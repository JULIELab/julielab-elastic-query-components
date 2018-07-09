package de.julielab.elastic.query.components.data.aggregation;

/**
 * Instances of this class will be ignored when creating the actual ElasticSearch query in {@link de.julielab.elastic.query.components.ElasticSearchComponent}.
 * This class can be useful in cases where there are cases where some algorithm should create an aggregation request
 * but cannot always do so, depending on its input. Then, it might just return an instance of this class.
 */
public class NoOpAggregation extends AggregationRequest {
}
