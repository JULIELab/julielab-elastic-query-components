package de.julielab.elastic.query.components;

import de.julielab.elastic.query.components.data.*;
import de.julielab.elastic.query.components.data.HighlightCommand.HlField;
import de.julielab.elastic.query.components.data.aggregation.*;
import de.julielab.elastic.query.components.data.aggregation.AggregationRequest.OrderCommand;
import de.julielab.elastic.query.components.data.query.*;
import de.julielab.elastic.query.components.data.query.FunctionScoreQuery.BoostMode;
import de.julielab.elastic.query.components.data.query.FunctionScoreQuery.FieldValueFactor;
import de.julielab.elastic.query.services.IElasticServerResponse;
import de.julielab.elastic.query.services.ISearchClientProvider;
import de.julielab.java.utilities.prerequisites.PrerequisiteChecker;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.MultiMatchQueryBuilder.Type;
import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder.Field;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Supplier;

import static org.elasticsearch.index.query.SimpleQueryStringFlag.*;

public class ElasticSearchComponent<C extends ElasticSearchCarrier<IElasticServerResponse>> extends AbstractSearchComponent<C> implements ISearchServerComponent<C> {

    // The following highlighting-defaults are taken from
    // http://www.elasticsearch.org/guide/reference/api/search/highlighting/
    // Default size of highlighting fragments
    private static final int DEFAULT_FRAGSIZE = 100;
    private static final int DEFAULT_NUMBER_FRAGS = 5;

    private RestHighLevelClient client;

    public ElasticSearchComponent(Logger log, ISearchClientProvider searchClientProvider) {
        super(log);
        log.info("Obtaining ElasticSearch client...");
        client = searchClientProvider.getSearchClient().getRestHighLevelClient();
        log.info("ElasticSearch client retrieved.");
    }

    @Override
    public boolean processSearch(C elasticSearchCarrier) {
        StopWatch w = new StopWatch();
        w.start();
        List<SearchServerRequest> serverRequests = elasticSearchCarrier.getServerRequests();
        PrerequisiteChecker.checkThat()
                .notNull((Supplier<?>) () -> serverRequests)
                .notEmpty(serverRequests)
                .withNames("Server requests", "Server requests")
                .execute();

        // It could
        // be that the search component occurs multiple times in a
        // search chain. But then, the last response(s) should have been
        // consumed by now.
        elasticSearchCarrier.clearSearchResponses();

        // One application search may result in multiple search server commands,
        // e.g. suggestions where for each facet suggestions are searched or for
        // B-terms where there are multiple search nodes.
        // We should just take care that the results are ordered in a parallel
        // way to the server commands, see at the end of the method.
        List<SearchRequest> searchRequests = new ArrayList<>(serverRequests.size());
        List<SearchRequest> suggestionBuilders = new ArrayList<>(serverRequests.size());
        log.debug("Number of search server commands: {}", serverRequests.size());
        final PrerequisiteChecker prChecker = PrerequisiteChecker.checkThat();
        try {

            for (int i = 0; i < serverRequests.size(); i++) {
                log.debug("Configuring ElasticSearch query for server command {}", i);

                SearchServerRequest serverRequest = serverRequests.get(i);
                prChecker.notNull((Supplier<?>) () -> serverRequest)
                        .notNull((Supplier<?>) () -> serverRequest.query)
                        .withNames("Server request " + i, "Server request query for request " + i);

                checkDeepPagingParameters(serverRequest);

                // If we have a deep pagination request with searchAfter, we need to create a "point in time" state
                // of the index first
                OpenPointInTimeResponse openPointInTimeResponse = null;
                if (serverRequest.downloadCompleteResultsMethod.equalsIgnoreCase("searchAfter")) {
                    openPointInTimeResponse = client.openPointInTime(new OpenPointInTimeRequest(serverRequest.index).keepAlive(TimeValue.parseTimeValue(serverRequest.downloadCompleteResultMethodKeepAlive, "DownloadAll.afterSearch.PIT")), RequestOptions.DEFAULT);
                }

                if (null != serverRequest.query) {
                    handleSearchRequest(searchRequests, openPointInTimeResponse, serverRequest);
                }
                if (null != serverRequest.suggestionText) {
                    handleSuggestionRequest(suggestionBuilders, serverRequest);
                }
            }
            prChecker.execute();

            // Send the query to the server
            if (searchRequests.size() == elasticSearchCarrier.getServerRequests().size()) {
//                final MultiSearchRequest msr = new MultiSearchRequest();
//                for (SearchRequest srb : searchRequests)
//                    msr.add(srb);
                log.debug("Issueing {} search request as a multi search", searchRequests.size());
//                final MultiSearchResponse multiSearchResponse = client.msearch(msr, RequestOptions.DEFAULT);
//                Item[] responses = multiSearchResponse.getResponses();
                List<SearchResponse> responses = new ArrayList<>();
                for (int i = 0; i < searchRequests.size(); i++) {
//                    Item item = responses[i];
//                    SearchResponse response = item.getResponse();
                    final boolean isCountRequest = serverRequests.get(i).isCountRequest;
                    SearchRequest sr = searchRequests.get(i);
                    ElasticServerResponse serverRsp;
                    try {
                        SearchResponse response = null;
                        CountResponse countResponse = null;
                        if (!isCountRequest) {
                            response = client.search(sr, RequestOptions.DEFAULT);
                            log.trace("Response from ElasticSearch: {}", response);
                        } else {
                            countResponse = client.count(new CountRequest(sr.indices(), sr.source().query()), RequestOptions.DEFAULT);
                            log.trace("Response from ElasticSearch: {}", countResponse);
                        }

                        serverRsp = new ElasticServerResponse(response, countResponse, serverRequests.get(i).downloadCompleteResults, serverRequests.get(i).downloadCompleteResultsLimit, searchRequests.get(i), client);
                        int status = isCountRequest ? countResponse.status().getStatus() : response.status().getStatus();
                        if (status > 299 && status < 200) {
                            serverRsp.setQueryError(QueryError.QUERY_ERROR);
//                        serverRsp.setQueryErrorMessage(item.getFailureMessage());
                            serverRsp.setQueryErrorMessage("HTTP status " + status);

                        }
                    } catch (IOException e) {
                        serverRsp = new ElasticServerResponse();
                        serverRsp.setQueryError(QueryError.NO_RESPONSE);
                        serverRsp.setQueryErrorMessage(e.getMessage());
                    }
                    elasticSearchCarrier.addSearchResponse(serverRsp);
                }
            } else {
                throw new IllegalStateException(
                        "There is at least one server request for which on ElasticSearch query could be created. This shouldn't happen.");
            }
            if (!suggestionBuilders.isEmpty()) {
                for (SearchRequest suggestBuilder : suggestionBuilders) {
                    SearchResponse suggestResponse = client.search(suggestBuilder, RequestOptions.DEFAULT);
                    elasticSearchCarrier.addSearchResponse(new ElasticServerResponse(suggestResponse, null, false, -1, null, client));
                }
            }
            w.stop();
            log.debug("ElasticSearch process took {}ms ({}s)", w.getTime(), w.getTime() / 1000);
        } catch (NoNodeAvailableException e) {
            log.error("No ElasticSearch node available: {}", e.getMessage());
            ElasticServerResponse serverRsp = new ElasticServerResponse();
            serverRsp.setQueryError(QueryError.NO_NODE_AVAILABLE);
            serverRsp.setQueryErrorMessage(e.getMessage());
            elasticSearchCarrier.addSearchResponse(serverRsp);
            return true;
        } catch (IOException e) {
            log.error("IOException occurred while searching", e);
            ElasticServerResponse serverRsp = new ElasticServerResponse();
            serverRsp.setQueryError(QueryError.QUERY_ERROR);
            serverRsp.setQueryErrorMessage(e.getMessage());
            elasticSearchCarrier.addSearchResponse(serverRsp);
            return true;
        }

        return false;
    }

    private void checkDeepPagingParameters(SearchServerRequest serverRequest) {
        if (!serverRequest.suppressDownloadCompleteResultPerformanceChecks && serverRequest.downloadCompleteResults) {
            final List<SortCommand> sortCmds = serverRequest.sortCmds;
            if (serverRequest.downloadCompleteResultsMethod.equalsIgnoreCase("scroll")) {
                if (!sortCmds.isEmpty()) {
                    for (SortCommand cmd : sortCmds) {
                        if (cmd.field.equals("_doc") && cmd.order == SortCommand.SortOrder.DESCENDING)
                            log.warn("All results are downloaded with a scroll cursor. However, the sorting on the _doc field is set in descending order. This makes scroll slow. Use ascending order instead. This warning can be disabled in code the SearchServerRequest object.");
                        else if (!cmd.field.equals("_doc"))
                            log.warn("All results are downloaded with a scroll cursor. However, the sorting field is set to " + cmd.field + ". This makes scroll slow. Use ascending order instead. This warning can be disabled in code the SearchServerRequest object.");
                    }
                }
            } else if (serverRequest.downloadCompleteResultsMethod.equalsIgnoreCase("searchAfter")) {
                if (sortCmds.isEmpty())
                    log.warn("All results are downloaded with a searchAfter cursor. However, no sorting is given. To make the retrieval more efficient, sort ascending (even though the ElasticSearch documentation says descending, our tests showed ascending was quicker) on the '_shard_doc' field. This warning can be disabled in code the SearchServerRequest object.");
                else {
                    for (SortCommand cmd : sortCmds) {
                        if (cmd.field.equals("_shard_doc") && cmd.order == SortCommand.SortOrder.DESCENDING)
                            log.warn("All results are downloaded with a searchAfter cursor. However, the sorting on the _shard_doc field is set in descending order. This makes searchAfter slow. Use ascending order instead. The ElasticSearch documentation says to sort descending but our tests showed that ascending was quicker. This warning can be disabled in code the SearchServerRequest object.");
                    }
                }
            }
        }
    }

    protected void handleSuggestionRequest(List<SearchRequest> suggestBuilders, SearchServerRequest serverCmd) {
        SuggestBuilder suggestBuilder = new SuggestBuilder().addSuggestion("",
                SuggestBuilders.completionSuggestion(serverCmd.suggestionField).text(serverCmd.suggestionText));

        final SearchSourceBuilder suggestSourceBuilder = new SearchSourceBuilder().suggest(suggestBuilder);
        final SearchRequest request = new SearchRequest(serverCmd.index).source(suggestSourceBuilder);

        suggestBuilders.add(request);
        if (log.isDebugEnabled())
            log.debug("Suggesting on index {}. Created search query \"{}\".", serverCmd.index,
                    suggestBuilder.toString());
    }

    protected void handleSearchRequest(List<SearchRequest> searchRequestBuilders,
                                       OpenPointInTimeResponse openPointInTimeResponse, SearchServerRequest serverCmd) {
        if (null == serverCmd.fieldsToReturn)
            serverCmd.addField("*");

        if (serverCmd.index == null)
            throw new IllegalArgumentException("The search command does not define an index to search on.");
        final SearchSourceBuilder ssb = new SearchSourceBuilder();
        final SearchRequest sr = new SearchRequest().source(ssb);

        // We cannot use PIT with an index, it is already given to the PIT request that is called before this method
        if(!serverCmd.downloadCompleteResultsMethod.equalsIgnoreCase("searchAfter"))
            sr.indices(serverCmd.index);

        log.trace("Searching on index {}", serverCmd.index);

        ssb.fetchSource(serverCmd.fetchSource);
        //ssb.explain(true)

        if (serverCmd.requestTimeout != null)
            ssb.timeout(TimeValue.parseTimeValue(serverCmd.requestTimeout, "RequestTimeout"));

        if (serverCmd.downloadCompleteResults) {
            if (serverCmd.downloadCompleteResultsMethod.equalsIgnoreCase("scroll"))
                sr.scroll(serverCmd.downloadCompleteResultMethodKeepAlive);
            else if (serverCmd.downloadCompleteResultsMethod.equalsIgnoreCase("searchAfter")) {
                if (openPointInTimeResponse == null)
                    throw new IllegalStateException("Download complete results is enabled but no point in time request was performed. This is coding error in this component.");
                ssb.pointInTimeBuilder(new PointInTimeBuilder(openPointInTimeResponse.getPointInTimeId()));
            } else
                throw new IllegalArgumentException("Unknown deep pagination method '" + serverCmd.downloadCompleteResultsMethod + "'.");
        }

        QueryBuilder queryBuilder = buildQuery(serverCmd.query);
        ssb.query(queryBuilder);


        if (null != serverCmd.fieldsToReturn)
            for (String field : serverCmd.fieldsToReturn) {
                ssb.storedField(field);
            }

        ssb.from(serverCmd.start);
        if (serverCmd.rows >= 0)
            ssb.size(serverCmd.rows);
        else
            ssb.size(0);

        if (serverCmd.trackTotalHitsUpTo != null)
            ssb.trackTotalHitsUpTo(serverCmd.trackTotalHitsUpTo);

        if (null != serverCmd.aggregationRequests) {
            for (AggregationRequest aggCmd : serverCmd.aggregationRequests.values()) {
                log.debug("Adding top aggregation command {} to query.", aggCmd.name);
                AbstractAggregationBuilder<?> aggregationBuilder = buildAggregation(aggCmd);
                if (aggregationBuilder != null)
                    ssb.aggregation(aggregationBuilder);
            }
        }

        if (null != serverCmd.hlCmds && serverCmd.hlCmds.size() > 0) {
            HighlightBuilder hb = new HighlightBuilder();
            ssb.highlighter(hb);
            for (int j = 0; j < serverCmd.hlCmds.size(); j++) {
                HighlightCommand hlc = serverCmd.hlCmds.get(j);
                for (HlField hlField : hlc.fields) {
                    Field field = new Field(hlField.field);
                    int fragsize = DEFAULT_FRAGSIZE;
                    int fragnum = DEFAULT_NUMBER_FRAGS;
                    if (hlField.type != null)
                        field.highlighterType(hlField.type);
                    if (!hlField.requirefieldmatch)
                        field.requireFieldMatch(false);
                    if (hlField.fragsize != Integer.MIN_VALUE)
                        fragsize = hlField.fragsize;
                    if (hlField.fragnum != Integer.MIN_VALUE)
                        fragnum = hlField.fragnum;
                    if (hlField.noMatchSize != Integer.MIN_VALUE)
                        field.noMatchSize(hlField.noMatchSize);
                    field.fragmentSize(fragsize);
                    field.numOfFragments(fragnum);
                    if (null != hlField.highlightQuery) {
                        field.highlightQuery(buildQuery(hlField.highlightQuery));
                    }
                    if (null != hlField.pre)
                        field.preTags(hlField.pre);
                    if (null != hlField.post)
                        field.postTags(hlField.post);
                    if (hlField.boundaryChars != null)
                        field.boundaryChars(hlField.boundaryChars);
                    if (hlField.boundaryScanner != null)
                        field.boundaryScannerType(hlField.boundaryScanner);
                    if (hlField.boundaryMaxScan != null)
                        field.boundaryMaxScan(hlField.boundaryMaxScan);
                    if (hlField.boundaryScannerLocale != null)
                        field.boundaryScannerLocale(hlField.boundaryScannerLocale);
                    if (hlField.fields != null)
                        field.matchedFields(hlField.fields);
                    field.forceSource(hlField.forceSource);
                    if (hlField.fragmenter != null)
                        field.fragmenter(hlField.fragmenter);
                    if (hlField.fragmentOffset != null)
                        field.fragmentOffset(hlField.fragmentOffset);
                    if (hlField.matchedFields != null)
                        field.matchedFields(hlField.matchedFields);
                    if (hlField.order != null)
                        field.order(hlField.order);
                    if (hlField.phraseLimit != null)
                        field.phraseLimit(hlField.phraseLimit);
                    hb.field(field);
                }
            }
            // srb.setHighlighterPreTags(preTags.toArray(new
            // String[preTags.size()]));
            // srb.setHighlighterPostTags(postTags.toArray(new
            // String[postTags.size()]));
        }

        if (null != serverCmd.sortCmds) {
            for (SortCommand sortCmd : serverCmd.sortCmds) {
                SortOrder sort;
                switch (sortCmd.order) {
                    case ASCENDING:
                        sort = SortOrder.ASC;
                        break;
                    case DESCENDING:
                        sort = SortOrder.DESC;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown sort order: " + sortCmd.order);
                }
                ssb.sort(sortCmd.field, sort);
            }
        } else if (serverCmd.downloadCompleteResults && serverCmd.downloadCompleteResultsMethod.equalsIgnoreCase("searchAfter"))
            throw new IllegalArgumentException("The searchAfter method is used for deep pagination but no sort command is given. A sort command is necessary on a unique field to be able to specify distinct result pages. Best performance is obtained by using the  internal _shard_doc field in descending order without tracking total hits.");

        if (null != serverCmd.postFilterQuery) {
            QueryBuilder postFilter = buildQuery(serverCmd.postFilterQuery);
            ssb.postFilter(postFilter);
        }

        searchRequestBuilders.add(sr);

        log.debug("Searching on index {}. Created search query \"{}\".", serverCmd.index, ssb);
    }

    protected AbstractAggregationBuilder<?> buildAggregation(AggregationRequest aggCmd) {
        if (NoOpAggregation.class.equals(aggCmd.getClass()))
            return null;
        if (TermsAggregation.class.equals(aggCmd.getClass())) {
            TermsAggregation termsAgg = (TermsAggregation) aggCmd;

            TermsAggregationBuilder termsBuilder = AggregationBuilders.terms(termsAgg.name).field(termsAgg.field);
            List<BucketOrder> compoundOrder = new ArrayList<>();
            for (OrderCommand orderCmd : termsAgg.order) {
                BucketOrder order = null;
                boolean ascending = false;
                if (null != orderCmd && null != orderCmd.sortOrder)
                    ascending = orderCmd.sortOrder == OrderCommand.SortOrder.ASCENDING;
                if (null != orderCmd) {
                    switch (orderCmd.referenceType) {
                        case AGGREGATION_MULTIVALUE:
                            order = BucketOrder.aggregation(orderCmd.referenceName, orderCmd.metric.name(), ascending);
                            break;
                        case AGGREGATION_SINGLE_VALUE:
                            order = BucketOrder.aggregation(orderCmd.referenceName, ascending);
                            break;
                        case COUNT:
                            order = BucketOrder.count(ascending);
                            break;
                        case TERM:
                            order = BucketOrder.key(ascending);
                            break;
                    }
                    if (null != order)
                        compoundOrder.add(order);
                }
            }
            if (!compoundOrder.isEmpty())
                termsBuilder.order(BucketOrder.compound(compoundOrder));
            if (null != termsAgg.size)
                termsBuilder.size(termsAgg.size);

            {
                // manage the in- or exclusion of terms into the aggregation
                String includeRegex = null;
                String excludeRegex = null;
                SortedSet<BytesRef> includeTerms = null;
                SortedSet<BytesRef> excludeTerms = null;
                if (termsAgg.include != null) {
                    if (termsAgg.include instanceof String) {
                        includeRegex = (String) termsAgg.include;
                    } else if (termsAgg.include.getClass().isArray()) {
                        includeTerms = new TreeSet<>();
                        for (int i = 0; i < Array.getLength(termsAgg.include); ++i) {
                            includeTerms.add(new BytesRef(String.valueOf(Array.get(termsAgg.include, i))));
                        }
                    } else {
                        includeTerms = new TreeSet<>();
                        for (Iterator<?> it = ((Collection<?>) termsAgg.include).iterator(); it.hasNext(); ) {
                            includeTerms.add(new BytesRef(String.valueOf(it.next())));
                        }
                    }
                }
                if (termsAgg.exclude != null) {
                    if (termsAgg.exclude instanceof String) {
                        excludeRegex = (String) termsAgg.exclude;
                    } else if (termsAgg.exclude.getClass().isArray()) {
                        excludeTerms = new TreeSet<>();
                        for (int i = 0; i < Array.getLength(termsAgg.exclude); ++i) {
                            excludeTerms.add(new BytesRef(String.valueOf(Array.get(termsAgg.exclude, i))));
                        }
                    } else {
                        excludeTerms = new TreeSet<>();
                        for (Iterator<?> it = ((Collection<?>) termsAgg.exclude).iterator(); it.hasNext(); ) {
                            excludeTerms.add(new BytesRef(String.valueOf(it.next())));
                        }
                    }
                }
                IncludeExclude includeExclude = null;
                if (includeRegex != null || excludeRegex != null)
                    includeExclude = new IncludeExclude(includeRegex, excludeRegex);
                else if ((includeTerms != null && !includeTerms.isEmpty())
                        || (excludeTerms != null && !excludeTerms.isEmpty()))
                    includeExclude = new IncludeExclude(includeTerms, excludeTerms);

                if (includeExclude != null)
                    termsBuilder.includeExclude(includeExclude);
                // End inclusion / exclusion of aggregation terms
            }

            // Add sub aggregations
            if (null != termsAgg.subaggregations) {
                for (AggregationRequest subAggCmd : termsAgg.subaggregations.values()) {
                    termsBuilder.subAggregation(buildAggregation(subAggCmd));
                }
            }
            return termsBuilder;
        }
        if (MaxAggregation.class.equals(aggCmd.getClass())) {
            MaxAggregation maxAgg = (MaxAggregation) aggCmd;
            MaxAggregationBuilder maxBuilder = AggregationBuilders.max(maxAgg.name);
            if (null != maxAgg.field)
                maxBuilder.field(maxAgg.field);
            if (null != maxAgg.script)
                maxBuilder.script(
                        new Script(ScriptType.INLINE, maxAgg.scriptLang.name(), maxAgg.script, Collections.emptyMap()));
            return maxBuilder;
        }
        if (TopHitsAggregation.class.equals(aggCmd.getClass())) {
            TopHitsAggregation topHitsAgg = (TopHitsAggregation) aggCmd;
            TopHitsAggregationBuilder topHitsBuilder = AggregationBuilders.topHits(topHitsAgg.name);
            String[] includes = null;
            if (null != topHitsAgg.includeFields)
                includes = topHitsAgg.includeFields.toArray(new String[0]);
            String[] excludes = null;
            if (null != topHitsAgg.excludeFields)
                excludes = topHitsAgg.excludeFields.toArray(new String[0]);
            if (null != includes || null != excludes)
                topHitsBuilder.fetchSource(includes, excludes);
            if (topHitsAgg.size != null)
                topHitsBuilder.size(topHitsAgg.size);
            return topHitsBuilder;
        }
        if (SignificantTermsAggregation.class.equals(aggCmd.getClass())) {
            SignificantTermsAggregation sigAgg = (SignificantTermsAggregation) aggCmd;
            SignificantTermsAggregationBuilder esSigAgg = AggregationBuilders.significantTerms(sigAgg.name);
            esSigAgg.field(sigAgg.field);
            return esSigAgg;
        }
        log.error("Unhandled aggregation command class: {}", aggCmd.getClass());
        return null;
    }

    protected QueryBuilder buildQuery(SearchServerQuery searchServerQuery) {
        if (null == searchServerQuery)
            throw new IllegalArgumentException("The search server query is null");
        QueryBuilder queryBuilder = null;
        if (LuceneSyntaxQuery.class.equals(searchServerQuery.getClass())) {
            LuceneSyntaxQuery luceneSyntaxQuery = (LuceneSyntaxQuery) searchServerQuery;
            queryBuilder = buildQueryStringQuery(luceneSyntaxQuery);
        } else if (MultiMatchQuery.class.equals(searchServerQuery.getClass())) {
            MultiMatchQuery query = (MultiMatchQuery) searchServerQuery;
            queryBuilder = buildMultiMatchQuery(query);
        } else if (MatchQuery.class.equals(searchServerQuery.getClass())) {
            queryBuilder = buildMatchQuery((MatchQuery) searchServerQuery);
        } else if (MatchAllQuery.class.equals(searchServerQuery.getClass())) {
            queryBuilder = new MatchAllQueryBuilder();
        } else if (BoolQuery.class.equals(searchServerQuery.getClass())) {
            queryBuilder = buildBoolQuery((BoolQuery) searchServerQuery);
        } else if (TermQuery.class.equals(searchServerQuery.getClass())) {
            queryBuilder = buildTermQuery((TermQuery) searchServerQuery);
        } else if (NestedQuery.class.equals(searchServerQuery.getClass())) {
            queryBuilder = buildNestedQuery((NestedQuery) searchServerQuery);
        } else if (FunctionScoreQuery.class.equals(searchServerQuery.getClass())) {
            queryBuilder = buildFunctionScoreQuery((FunctionScoreQuery) searchServerQuery);
        } else if (ConstantScoreQuery.class.equals(searchServerQuery.getClass())) {
            queryBuilder = buildConstantScoreQuery((ConstantScoreQuery) searchServerQuery);
        } else if (MatchPhraseQuery.class.equals(searchServerQuery.getClass())) {
            queryBuilder = buildMatchPhraseQuery((MatchPhraseQuery) searchServerQuery);
        } else if (TermsQuery.class.equals(searchServerQuery.getClass())) {
            queryBuilder = buildTermsQuery((TermsQuery) searchServerQuery);
        } else if (WildcardQuery.class.equals(searchServerQuery.getClass())) {
            queryBuilder = buildWildcardQuery((WildcardQuery) searchServerQuery);
        } else if (SimpleQueryStringQuery.class.equals(searchServerQuery.getClass())) {
            queryBuilder = buildSimpleQueryStringQuery((SimpleQueryStringQuery) searchServerQuery);
        } else if (RangeQuery.class.equals(searchServerQuery.getClass())) {
            queryBuilder = buildRangeQuery((RangeQuery) searchServerQuery);
        } else {
            throw new IllegalArgumentException("Unhandled query type: " + searchServerQuery.getClass());
        }
        return queryBuilder;
    }

    private QueryBuilder buildRangeQuery(RangeQuery rangeQuery) {
        final RangeQueryBuilder builder = QueryBuilders.rangeQuery(rangeQuery.field);
        if (rangeQuery.lessThan != null)
            builder.to(rangeQuery.lessThan);
        else if (rangeQuery.lessThanOrEqual != null)
            builder.to(rangeQuery.lessThanOrEqual, true);
        if (rangeQuery.greaterThan != null)
            builder.from(rangeQuery.greaterThan);
        else if (rangeQuery.greaterThanOrEqual != null)
            builder.from(rangeQuery.greaterThanOrEqual, true);
        if (rangeQuery.format != null)
            builder.format(rangeQuery.format);
        if (rangeQuery.timeZone != null)
            builder.timeZone(rangeQuery.timeZone);
        if (rangeQuery.relation != null)
            builder.relation(rangeQuery.relation.name());

        if (rangeQuery.boost != 1f)
            builder.boost(rangeQuery.boost);

        return builder;
    }

    private QueryBuilder buildSimpleQueryStringQuery(SimpleQueryStringQuery simpleQueryStringQuery) {
        final SimpleQueryStringBuilder builder = QueryBuilders.simpleQueryStringQuery(simpleQueryStringQuery.query);
        if (simpleQueryStringQuery.fields != null && simpleQueryStringQuery.fieldBoosts != null && simpleQueryStringQuery.fields.size() != simpleQueryStringQuery.fieldBoosts.size() && !simpleQueryStringQuery.fieldBoosts.isEmpty())
            throw new IllegalArgumentException("For the SimpleQueryStringQuery either each field must be given a boost via the fieldBoosts field or no boost must be given at all. However, there are " + simpleQueryStringQuery.fields.size() + " fields given and " + simpleQueryStringQuery.fieldBoosts.size() + " field boosts.");
        for (int i = 0; i < simpleQueryStringQuery.fields.size(); i++) {
            float boost = 1f;
            if (simpleQueryStringQuery.fieldBoosts != null && !simpleQueryStringQuery.fieldBoosts.isEmpty())
                boost = simpleQueryStringQuery.fieldBoosts.get(i);
            builder.field(simpleQueryStringQuery.fields.get(i), boost);
        }
        if (simpleQueryStringQuery.defaultOperator != null) {
            builder.defaultOperator(Operator.valueOf(simpleQueryStringQuery.defaultOperator.name()));
        }
        builder.analyzer(simpleQueryStringQuery.analyzer);
        if (simpleQueryStringQuery.flags != null) {
            SimpleQueryStringFlag[] esFlags = new SimpleQueryStringFlag[simpleQueryStringQuery.flags.size()];
            for (int i = 0; i < simpleQueryStringQuery.flags.size(); i++) {
                SimpleQueryStringFlag esFlag;
                switch (simpleQueryStringQuery.flags.get(i)) {
                    case ALL:
                        esFlag = ALL;
                        break;
                    case NONE:
                        esFlag = NONE;
                        break;
                    case AND:
                        esFlag = AND;
                        break;
                    case NOT:
                        esFlag = NOT;
                        break;
                    case OR:
                        esFlag = OR;
                        break;
                    case PREFIX:
                        esFlag = PREFIX;
                        break;
                    case PHRASE:
                        esFlag = PREFIX;
                        break;
                    case PRECEDENCE:
                        esFlag = PRECEDENCE;
                        break;
                    case ESCAPE:
                        esFlag = ESCAPE;
                        break;
                    case WHITESPACE:
                        esFlag = WHITESPACE;
                        break;
                    case FUZZY:
                        esFlag = FUZZY;
                        break;
                    case NEAR:
                        esFlag = NEAR;
                        break;
                    case SLOP:
                        esFlag = SLOP;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown flag for the SimpleQueryStringQuery: " + simpleQueryStringQuery.flags.get(i));
                }
                esFlags[i] = esFlag;
            }
            builder.flags(esFlags);
        }
        builder.analyzeWildcard(simpleQueryStringQuery.analyzeWildcard);
        builder.lenient(simpleQueryStringQuery.lenient);
        builder.minimumShouldMatch(simpleQueryStringQuery.minimumShouldMatch);
        builder.quoteFieldSuffix(simpleQueryStringQuery.quoteFieldSuffix);
        return builder;
    }

    private QueryBuilder buildWildcardQuery(WildcardQuery wildcardQuery) {
        WildcardQueryBuilder esWildcardQuery = QueryBuilders.wildcardQuery(wildcardQuery.field, wildcardQuery.query);
        if (wildcardQuery.boost != 1f)
            esWildcardQuery.boost(wildcardQuery.boost);
        return esWildcardQuery;
    }

    private QueryBuilder buildTermsQuery(TermsQuery termsQuery) {
        TermsQueryBuilder esTermsQueryBuilder = QueryBuilders.termsQuery(termsQuery.field, termsQuery.terms);
        if (termsQuery.boost != 1f)
            esTermsQueryBuilder.boost(termsQuery.boost);
        return esTermsQueryBuilder;
    }

    private QueryBuilder buildMatchPhraseQuery(MatchPhraseQuery matchPhraseQuery) {
        MatchPhraseQueryBuilder builder = QueryBuilders.matchPhraseQuery(matchPhraseQuery.field,
                matchPhraseQuery.phrase);
        builder.slop(matchPhraseQuery.slop);
        if (matchPhraseQuery.boost != 1f)
            builder.boost(matchPhraseQuery.boost);
        return builder;
    }

    private QueryBuilder buildConstantScoreQuery(ConstantScoreQuery constantScoreQuery) {
        ConstantScoreQueryBuilder esConstantScoreQuery = QueryBuilders
                .constantScoreQuery(buildQuery(constantScoreQuery.query));
        esConstantScoreQuery.boost(constantScoreQuery.boost);
        return esConstantScoreQuery;
    }

    private QueryBuilder buildFunctionScoreQuery(FunctionScoreQuery functionScoreQuery) {
        SearchServerQuery scoredQuery = functionScoreQuery.query;
        FieldValueFactor fieldValueFactor = functionScoreQuery.fieldValueFactor;
        BoostMode boostMode = functionScoreQuery.boostMode;
        float boost = functionScoreQuery.boost;
        if (null == scoredQuery)
            throw new IllegalArgumentException("Currently, only a single query for FunctionScoreQuery is supported");
        if (null == fieldValueFactor)
            throw new IllegalArgumentException(
                    "Currently, only the fieldValueFactor function is supported for FunctionScoreQuery, but the fieldValueFactor was null.");
        QueryBuilder esScoredQuery = buildQuery(scoredQuery);
        FieldValueFactorFunctionBuilder esFieldValueFactor = ScoreFunctionBuilders
                .fieldValueFactorFunction(fieldValueFactor.field);
        FieldValueFactorFunction.Modifier esModifier = FieldValueFactorFunction.Modifier
                .valueOf(fieldValueFactor.modifier.name());

        esFieldValueFactor.factor(fieldValueFactor.factor);
        esFieldValueFactor.modifier(esModifier);
        esFieldValueFactor.missing(fieldValueFactor.missing);

        FunctionScoreQueryBuilder esFunctionScoreQuery = QueryBuilders.functionScoreQuery(esScoredQuery,
                esFieldValueFactor);
        esFunctionScoreQuery.boost(boost);
        esFunctionScoreQuery.boostMode(CombineFunction.fromString(boostMode.name()));

        return esFunctionScoreQuery;
    }

    private QueryBuilder buildNestedQuery(NestedQuery nestedQuery) {
        QueryBuilder esQuery = buildQuery(nestedQuery.query);
        NestedQueryBuilder nestedEsQuery = QueryBuilders.nestedQuery(nestedQuery.path, esQuery,
                ScoreMode.valueOf(StringUtils.capitalize(nestedQuery.scoreMode.name())));
        if (null != nestedQuery.innerHits) {
            InnerHitBuilder innerHitBuilder = new InnerHitBuilder();
            if (nestedQuery.innerHits.fetchSource)
                innerHitBuilder.setFetchSourceContext(FetchSourceContext.FETCH_SOURCE);
            else
                innerHitBuilder.setFetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
            innerHitBuilder.setStoredFieldNames(nestedQuery.innerHits.fields);
            if (nestedQuery.innerHits.highlight != null) {
                HighlightBuilder hb = new HighlightBuilder();
                innerHitBuilder.setHighlightBuilder(hb);
                HighlightCommand innerHl = nestedQuery.innerHits.highlight;
                for (HlField hlField : innerHl.fields) {
                    Field esHlField = new Field(hlField.field);
                    esHlField.fragmentSize(hlField.fragsize);
                    esHlField.numOfFragments(hlField.fragnum);
                    if (null != hlField.pre)
                        esHlField.preTags(hlField.pre);
                    if (null != hlField.post)
                        esHlField.postTags(hlField.post);
                    hb.field(esHlField);
                }
            }
            innerHitBuilder.setExplain(nestedQuery.innerHits.explain);
            if (null != nestedQuery.innerHits.size)
                innerHitBuilder.setSize(nestedQuery.innerHits.size);
            innerHitBuilder.setIgnoreUnmapped(true);
            nestedEsQuery.innerHit(innerHitBuilder);
        }
        return nestedEsQuery;
    }

    private QueryBuilder buildMatchQuery(MatchQuery matchQuery) {
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(matchQuery.field, matchQuery.query);
        switch (matchQuery.operator.toLowerCase()) {
            case "or":
                matchQueryBuilder.operator(Operator.OR);
                break;
            case "and":
                matchQueryBuilder.operator(Operator.AND);
                break;
        }
        if (null != matchQuery.analyzer)
            matchQueryBuilder.analyzer(matchQuery.analyzer);
        if (matchQuery.boost != 1f)
            matchQueryBuilder.boost(matchQuery.boost);
        if (!StringUtils.isBlank(matchQuery.minimumShouldMatch))
            matchQueryBuilder.minimumShouldMatch(matchQuery.minimumShouldMatch);
        if (!StringUtils.isBlank(matchQuery.fuzzyRewrite))
            matchQueryBuilder.fuzzyRewrite(matchQuery.fuzzyRewrite);
        matchQueryBuilder.fuzzyTranspositions(matchQuery.allowFuzzyTranspositions);
        return matchQueryBuilder;
    }

    protected QueryBuilder buildQueryStringQuery(LuceneSyntaxQuery luceneSyntaxQuery) {
        QueryBuilder queryBuilder;
        String queryString;
        queryString = luceneSyntaxQuery.query;
        // No fields given, so we assume a query string in Lucene syntax
        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery(queryString);

        if (null != luceneSyntaxQuery.analyzer)
            queryStringQueryBuilder.analyzer(luceneSyntaxQuery.analyzer);
        queryBuilder = queryStringQueryBuilder;
        return queryBuilder;
    }

    protected QueryBuilder buildMultiMatchQuery(MultiMatchQuery query) {
        log.trace("Building multi match query (a match query over multiple fields).");
        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(query.query);
        multiMatchQueryBuilder.operator(Operator.valueOf(query.operator.toUpperCase()));
        for (int i = 0; i < query.fields.size(); i++) {
            String field = query.fields.get(i);
            Float weight;
            if (null != query.fieldWeights) {
                weight = query.fieldWeights.get(i);
                multiMatchQueryBuilder.field(field, weight);
            } else {
                multiMatchQueryBuilder.field(field);
            }
        }
        if (null != query.type) {
            MultiMatchQueryBuilder.Type multiFieldMatchType = null;
            switch (query.type) {
                case best_fields:
                    multiFieldMatchType = Type.BEST_FIELDS;
                    break;
                case cross_fields:
                    multiFieldMatchType = Type.CROSS_FIELDS;
                    break;
                case most_fields:
                    multiFieldMatchType = Type.MOST_FIELDS;
                    break;
                case phrase:
                    multiFieldMatchType = Type.PHRASE;
                    break;
                case phrase_prefix:
                    multiFieldMatchType = Type.PHRASE_PREFIX;
                    break;
            }
            multiMatchQueryBuilder.type(multiFieldMatchType);
        }
        return multiMatchQueryBuilder;
    }

    private BoolQueryBuilder buildBoolQuery(BoolQuery query) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        if (query.clauses == null || query.clauses.isEmpty())
            throw new IllegalStateException("A BoolQuery without any query clauses was given.");
        for (BoolClause clause : query.clauses) {
            if (clause.occur == null)
                throw new IllegalStateException("Encountered boolean query clause without a set \"occur\" property.");
            for (SearchServerQuery searchServerQuery : clause.queries) {
                QueryBuilder clauseQuery = buildQuery(searchServerQuery);
                if (clauseQuery == null)
                    continue;
                switch (clause.occur) {
                    case MUST:
                        boolQueryBuilder.must(clauseQuery);
                        break;
                    case SHOULD:
                        boolQueryBuilder.should(clauseQuery);
                        break;
                    case MUST_NOT:
                        boolQueryBuilder.mustNot(clauseQuery);
                        break;
                    case FILTER:
                        boolQueryBuilder.filter(clauseQuery);
                        break;
                }
            }
        }
        if (query.boost != 1f)
            boolQueryBuilder.boost(query.boost);
        if (!StringUtils.isBlank(query.minimumShouldMatch))
            boolQueryBuilder.minimumShouldMatch(query.minimumShouldMatch);
        return boolQueryBuilder;
    }

    public TermQueryBuilder buildTermQuery(TermQuery query) {
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder(query.field, query.term);
        return termQueryBuilder;
    }
}
