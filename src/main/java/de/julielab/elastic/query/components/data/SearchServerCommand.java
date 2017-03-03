/**
 * SolrSearchCommand.java
 *
 * Copyright (c) 2013, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 *
 * Author: faessler
 *
 * Current version: 1.0
 * Since version:   1.0
 *
 * Creation date: 06.04.2013
 **/

/**
 * 
 */
package de.julielab.elastic.query.components.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Multimap;

import de.julielab.elastic.query.components.data.SortCommand.SortOrder;
import de.julielab.elastic.query.components.data.aggregation.AggregationCommand;
import de.julielab.elastic.query.components.data.query.SearchServerQuery;

/**
 * @author faessler
 * 
 */
public class SearchServerCommand {

	/**
	 * A structured search server query. The actual query instance is a subclass
	 * of {@link SearchServerQuery}. This object has to be cast to its actual
	 * class and does then expose all properties of the server query.
	 * 
	 * @see SearchServerQuery
	 */
	public SearchServerQuery query;
	/**
	 * For auto completion, this field exposes the fragment to get suggestions
	 * for.
	 */
	public String suggestionText;
	/**
	 * The field for which to get suggestions.
	 */
	public String suggestionField;
	/**
	 * For some suggester types, e.g. completion suggester: The suggestion
	 * categories.
	 */
	public Multimap<String, String> suggestionCategories;
	public int start;
	/**
	 * The value Integer.MIN_VALUE means "not set".
	 */
	public int rows = 10;
	/**
	 * The fields for which their original content should be returned. Does only
	 * work for stored fields, of course. A null value (default) causes all
	 * fields to be returned. To return no fields, set an empty list.
	 */
	public List<String> fieldsToReturn;
	/**
	 * For multi-field queries, provided the fields that should be queried on
	 * using {@link #serverQuery}. The <tt>*</tt> wildcard is allowed for the
	 * field names.
	 */
	public boolean fetchSource;

	public List<FacetCommand> facetCmds;
	public Map<String, AggregationCommand> aggregationCmds;
	public List<HighlightCommand> hlCmds;
	// TODO should go into semedico as this is not general enough
	public boolean filterReviews;
	public List<SortCommand> sortCmds;
	/**
	 * The index to perform the search on.
	 * 
	 */
	public String index;
	/**
	 * Specifies a limit of documents to retrieve for this search.
	 */
	public int limit;
	/**
	 * Maps a name to a query for easy access, e.g. for highlighting. Specific
	 * (sub-)queries may be stored in this map.
	 */
	public Map<String, SearchServerQuery> namedQueries;
	public SearchServerQuery postFilterQuery;
	public List<String> indexTypes;

	public void addField(String field) {
		if (null == fieldsToReturn)
			fieldsToReturn = new ArrayList<String>();
		fieldsToReturn.add(field);
	}

	/**
	 * @param fc
	 */
	public void addFacetCommand(FacetCommand fc) {
		if (null == facetCmds)
			facetCmds = new ArrayList<FacetCommand>();
		facetCmds.add(fc);
	}

	/**
	 * @param hlc
	 */
	public void addHighlightCmd(HighlightCommand hlc) {
		if (null == hlCmds)
			hlCmds = new ArrayList<HighlightCommand>();
		hlCmds.add(hlc);

	}

	public void addSortCommand(String field, SortOrder order) {
		if (null == sortCmds)
			sortCmds = new ArrayList<SortCommand>();
		sortCmds.add(new SortCommand(field, order));
	}

	public void addAggregationCommand(AggregationCommand aggCmd) {
		if (null == aggregationCmds)
			aggregationCmds = new HashMap<>();
		aggregationCmds.put(aggCmd.name, aggCmd);
	}
}