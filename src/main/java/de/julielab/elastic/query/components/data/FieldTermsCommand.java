package de.julielab.elastic.query.components.data;

import de.julielab.elastic.query.components.data.aggregation.AggregationCommand.OrderCommand;

public class FieldTermsCommand {
	
	public enum OrderType {TERM, COUNT, DOC_SCORE}
	
	public String field;
	public int size;
	public OrderCommand.SortOrder[] sortOrders;
	// TODO document
	public OrderType[] orderTypes;
}
