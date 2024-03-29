/**
 * SolrSearchCommand.java
 * <p>
 * Copyright (c) 2013, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 * <p>
 * Author: faessler
 * <p>
 * Current version: 1.0
 * Since version:   1.0
 * <p>
 * Creation date: 06.04.2013
 */

/**
 *
 */
package de.julielab.elastic.query.components.data;

import de.julielab.elastic.query.components.data.SortCommand.SortOrder;
import de.julielab.elastic.query.components.data.aggregation.AggregationRequest;
import de.julielab.elastic.query.components.data.query.SearchServerQuery;

import java.util.*;

/**
 * @author faessler
 *
 */
public class SearchServerRequest {

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
    public Map<String, List<String>> suggestionCategories;
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
    public Collection<String> fieldsToReturn;
    /**
     * For multi-field queries, provided the fields that should be queried on
     * using {@link #serverQuery}. The <tt>*</tt> wildcard is allowed for the
     * field names.
     */
    public boolean fetchSource;

    public boolean isCountRequest;

    public Map<String, AggregationRequest> aggregationRequests;
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
    /**
     * Causes the return of all results of the query, not only a page or a
     * batch. May require additional requests to the server which are
     * automatically done by the
     * {@link ElasticServerResponse#getDocumentResults()}.
     */
    public boolean downloadCompleteResults;
    /**
     * <p>How to perform deep pagination to obtain all result documents. Either 'scroll' or 'searchAfter' (default).</p>
     * <p>Newer versions of ElasticSearch deprecate 'scroll' and recommend 'searchAfter' which is integrated right into
     * Lucene itself. For optimal performance, a sorting be defined with a specific value for each method.
     * 'scroll' works best with sort on '_doc' ascending while 'searchAfter' works best with sort order set to '_shard_doc' ascending
     * and no tracking of total hits. Note that the ElasticSearch documentation says the sorting on '_shard_doc' should be in descending order
     * but in our tests, ascending was much quicker. Even consistently a bit quicker than scroll.</p>
     *
     */
    public String downloadCompleteResultsMethod = "scroll";
    /**
     * Limits the maximum documents returned via deep pagination.
     */
    public int downloadCompleteResultsLimit = Integer.MAX_VALUE;
    /**
     * <p>Used when {@link #downloadCompleteResultsMethod} is 'searchAfter'. Denotes index of the sort command
     * in {@link #sortCmds} that should be used for the searchAfter pagination.</p>
     * <p>
     *     The searchAfter method works by querying the next batch of documents after the last document of the previous
     *     batch. The document position is determined by a unique sorting value. Thus, the field on which the
     *     sort command is applied must have unique values (hint: best performance is obtained using the internal
     *     _shard_doc field in descending order). To retrieve the correct value from the result, this
     *     field must be set to the sort command that specifies that sorting.
     * </p>
     */
    //public int downloadCompleteResultsSortValueIndex = 0;
    /**
     * <p>Keep alive time for deep pagination. Sets the time for which the original index state is kept for subsequent requests.</p>
     * <p>The format is described in the ElasticSearch documentation.</p>
     * @see <url>https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-datetime.html</url>
     */
    public String downloadCompleteResultMethodKeepAlive = "1m";
    /**
     * Is set to true, the {@link de.julielab.elastic.query.components.ElasticSearchComponent} checks the settings for
     * optimal performance and logs warning message if non-optimal settings are detected.
     */
    public boolean suppressDownloadCompleteResultPerformanceChecks = false;
    /**
     * The number of hits to track with accuracy. Specify Integer.MAX_VALUE if you need total accurate hit counts.
     */
    public Integer trackTotalHitsUpTo;

    /**
     * <p>The format is described in the ElasticSearch documentation.</p>
     * @see <url>https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-datetime.html</url>
     */
    public String requestTimeout;

    public void addField(String field) {
        if (null == fieldsToReturn)
            fieldsToReturn = new ArrayList<String>();
        fieldsToReturn.add(field);
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

    public void addAggregationCommand(AggregationRequest aggCmd) {
        if (null == aggregationRequests)
            aggregationRequests = new HashMap<>();
        aggregationRequests.put(aggCmd.name, aggCmd);
    }
}