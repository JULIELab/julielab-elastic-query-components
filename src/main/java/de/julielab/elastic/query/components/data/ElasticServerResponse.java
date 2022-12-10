package de.julielab.elastic.query.components.data;

import de.julielab.elastic.query.components.data.aggregation.*;
import de.julielab.elastic.query.services.IElasticServerResponse;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ElasticServerResponse implements IElasticServerResponse {


    private static final Logger log = LoggerFactory.getLogger(ElasticServerResponse.class);

    protected SearchResponse response;
    protected SearchResponse scrollResponse;
    protected boolean searchServerNotReachable;
    protected boolean isSuggestionSearchResponse;
    protected Suggest suggest;
    protected Map<String, Aggregation> aggregationsByName;
    protected QueryError queryError;
    protected RestHighLevelClient client;
    protected String queryErrorMessage;
    private boolean downloadCompleteResults;
    private int downloadCompleteResultsLimit;
    private SearchRequest searchRequest;
    private CountResponse countResponse;


    public ElasticServerResponse(SearchResponse response, CountResponse countResponse, boolean downloadCompleteResults, int downloadCompleteResultsLimit, SearchRequest searchRequest, RestHighLevelClient client) {
        this.response = response;
        this.countResponse = countResponse;
        this.downloadCompleteResults = downloadCompleteResults;
        this.downloadCompleteResultsLimit = downloadCompleteResultsLimit;
        this.searchRequest = searchRequest;
        this.client = client;
        if (response != null) {
            this.suggest = response.getSuggest();
            if (null != response.getAggregations())
                this.aggregationsByName = response.getAggregations().asMap();
        }

    }

    public ElasticServerResponse() {
    }

    public SearchResponse getResponse() {
        return response;
    }

    public CountResponse getCountResponse() {
        return countResponse;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public IAggregationResult getAggregationResult(AggregationRequest aggCmd) {
        String aggName = aggCmd.name;
        if (null == aggregationsByName || null == aggregationsByName.get(aggName)) {
            log.warn("No aggregation result with name \"{}\" was found in ElasticSearch's response.", aggName);
            return null;
        }
        Aggregation aggregation = aggregationsByName.get(aggName);

        return buildAggregationResult(aggCmd, aggregation);
    }

    public boolean hasAggregationResults() {
        return aggregationsByName != null && !aggregationsByName.isEmpty();
    }

    public boolean hasAggregation(String name) {
        return aggregationsByName != null && aggregationsByName.containsKey(name);
    }

    private IAggregationResult buildAggregationResult(AggregationRequest aggCmd, Aggregation aggregation) {
        if (TermsAggregation.class.equals(aggCmd.getClass())) {
            log.trace("Building {}", TermsAggregationResult.class.getSimpleName());
            TermsAggregation semedicoTermsAggregation = (TermsAggregation) aggCmd;
            TermsAggregationResult termsAggResult = new TermsAggregationResult();
            termsAggResult.setName(aggCmd.name);
            Terms esTermAggregation = (Terms) aggregation;
            for (Bucket bucket : esTermAggregation.getBuckets()) {
                long docCount = bucket.getDocCount();
                String term = (String) bucket.getKey();

                TermsAggregationUnit termsAggUnit = new TermsAggregationUnit();
                termsAggUnit.setCount(docCount);
                termsAggUnit.setTerm(term);

                for (Aggregation esSubAgg : bucket.getAggregations()) {
                    String esSubAggName = esSubAgg.getName();
                    AggregationRequest subAggCmd = semedicoTermsAggregation.getSubaggregation(esSubAggName);
                    termsAggUnit.addSubaggregationResult(buildAggregationResult(subAggCmd, esSubAgg));
                }
                termsAggResult.addAggregationUnit(termsAggUnit);
            }
            log.trace("Got {} aggregation units for terms aggregation.",
                    termsAggResult.getAggregationUnits() == null ? 0 : termsAggResult.getAggregationUnits().size());
            return termsAggResult;
        }
        if (MaxAggregation.class.equals(aggCmd.getClass())) {
            log.trace("Building {}", MaxAggregation.class.getSimpleName());
            Max esMaxAggregation = (Max) aggregation;
            MaxAggregationResult maxAggResult = new MaxAggregationResult();
            maxAggResult.setName(esMaxAggregation.getName());
            maxAggResult.setValue(esMaxAggregation.getValue());
            return maxAggResult;
        }
        if (SignificantTermsAggregation.class.equals(aggCmd.getClass())) {
            log.trace("Building {}", SignificantTermsAggregation.class.getSimpleName());
            SignificantTerms esSigAgg = (SignificantTerms) aggregation;
            SignificantTermsAggregationResult sigResult = new SignificantTermsAggregationResult();
            sigResult.setName(esSigAgg.getName());
            for (SignificantTerms.Bucket bucket : esSigAgg.getBuckets()) {
                String termId = (String) bucket.getKey();
                long docCount = bucket.getDocCount();
                SignificantTermsAggregationUnit aggUnit = new SignificantTermsAggregationUnit();
                aggUnit.setTerm(termId);
                aggUnit.setDocCount(docCount);
                sigResult.addAggregationUnit(aggUnit);
            }
            return sigResult;
        }
        if (TopHitsAggregation.class.equals(aggCmd.getClass())) {
            log.trace("Building {}", TopHitsAggregation.class.getSimpleName());
            TopHitsAggregationResult topHitsResult = new TopHitsAggregationResult();
            topHitsResult.setName(aggCmd.name);
            TopHits esTopHitsAggregation = (TopHits) aggregation;
            long totalHits = esTopHitsAggregation.getHits().getTotalHits().value;
            topHitsResult.setTotalHits(totalHits);
            SearchHits searchHits = esTopHitsAggregation.getHits();
            for (int i = 0; i < searchHits.getHits().length; i++) {
                final SearchHit searchHit = searchHits.getHits()[i];
                // For the ElasticSearch TopHitsAggregation we don't get the
                // stored fields back but the _source field
                // (see ElasticSearch documentation).
                // final Map<String, Object> source = searchHit.getSource();
                final Map<String, DocumentField> source = searchHit.getFields();

                ISearchServerDocument serverDoc = new ISearchServerDocument() {

                    @Override
                    public Optional<List<Object>> getFieldValues(String fieldName) {
                        return Optional.ofNullable(source.get(fieldName).getValues());
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public <V> Optional<V> getFieldValue(String fieldName) {
                        return Optional.ofNullable(source.get(fieldName).getValue());
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public <V> Optional<V> get(String fieldName) {
                        return Optional.ofNullable((source.get(fieldName).getValue()));
                    }

                    @Override
                    public String getId() {
                        return searchHit.getId();
                    }

                    @Override
                    public float getScore() {
                        return searchHit.getScore();
                    }

                };

                topHitsResult.addTopHitsDocument(serverDoc);
            }
            return topHitsResult;
        }
        return null;
    }


    @Override
    public Stream<ISearchServerDocument> getDocumentResults() {
        if (searchServerNotReachable) {
            log.debug("Not returning any document results because the server was not reachable.");
            return Stream.empty();
        }

        Iterator<ISearchServerDocument> documentIt = new Iterator<>() {

            private int pos = 0;
            private int documentsReturned = 0;
            private SearchHit[] currentHits = response.getHits().getHits();
            private String pointInTimeId = response.pointInTimeId();
            private Object[] lastSortValues = currentHits != null && currentHits.length > 0 ? currentHits[currentHits.length - 1].getSortValues() : null;

            @Override
            public boolean hasNext() {
                if (currentHits.length > 0 && documentsReturned < downloadCompleteResultsLimit) {
                    try {
                        // pointInTime and scroll are indicators for two different types of deep pagination.
                        // PointInTime is used with searchAfter which is preferred.
                        SearchResponse currentResponse = scrollResponse != null ? scrollResponse : response;
                        String scrollId = currentResponse.getScrollId() != null ? currentResponse.getScrollId() : response.getScrollId();
                        if (pos < currentHits.length) {
                            log.trace("There are more documents in the current response.");
                            return true;
                        } else if (!StringUtils.isBlank(scrollId)) {
                            log.debug(
                                    "No more documents present in the current response but got scroll ID {}. Querying next batch.",
                                    scrollId);
                            SearchResponse sr = client.scroll(new SearchScrollRequest(scrollId).scroll(TimeValue.timeValueMinutes(5)), RequestOptions.DEFAULT);
                            currentHits = sr.getHits().getHits();
                            log.trace("Received {} new hits from scroll request.", currentHits.length);
                            pos = 0;
                            scrollResponse = sr;
                            if (currentHits.length > 0)
                                return true;
                        } else if (pointInTimeId != null && downloadCompleteResults) {
                            log.debug(
                                    "No more documents present in the current response but PIT ID {}. Querying next batch.",
                                    pointInTimeId);
                            final SearchSourceBuilder sourceBuilder = searchRequest.source();
                            sourceBuilder.pointInTimeBuilder(new PointInTimeBuilder(pointInTimeId));
                            sourceBuilder.searchAfter(lastSortValues);
                            SearchResponse sr = client.search(searchRequest, RequestOptions.DEFAULT);
                            currentHits = sr.getHits().getHits();
                            lastSortValues = currentHits != null && currentHits.length > 0 ? currentHits[currentHits.length - 1].getSortValues() : null;
                            log.trace("Received {} new hits with searchAfter.", currentHits.length);
                            pos = 0;
                            scrollResponse = sr;
                            // update point in time ID
                            pointInTimeId = scrollResponse.pointInTimeId();
                            if (currentHits.length > 0)
                                return true;
                        }
                    } catch (IOException e) {
                        log.error("Could not retrieve the next batch of documents", e);
                    }
                }
                if (documentsReturned < downloadCompleteResultsLimit)
                    log.debug("No more hits returned from scrolling request.");
                else
                    log.debug("Hit the deep pagination limit of {}. Closing the request.", downloadCompleteResultsLimit);
                try {
                    pos = Integer.MAX_VALUE;
                    if (response.getScrollId() != null) {
                        log.debug("Closing the scroll with ID {}", response.getScrollId());
                        final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                        clearScrollRequest.addScrollId(response.getScrollId());
                        final boolean succeeded = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT).isSucceeded();
                        log.debug("Closing of scroll did succeed: {}", succeeded);
                    }
                    if (pointInTimeId != null && downloadCompleteResults) {
                        log.debug("Closing point of time (PIT) with ID {}", pointInTimeId);
                        final ClosePointInTimeRequest closePointInTimeRequest = new ClosePointInTimeRequest(pointInTimeId);
                        final boolean succeeded = client.closePointInTime(closePointInTimeRequest, RequestOptions.DEFAULT).isSucceeded();
                        log.debug("Closing of point in time did succeed: {}", succeeded);
                    }
                } catch (IOException e) {
                    log.error("Could not close scroll.", e);
                }
                return false;
            }

            @Override
            public ISearchServerDocument next() {
                if (!hasNext())
                    return null;
                log.trace("Returning next document at position {} of the current scroll batch.", pos);
                SearchHit hit = currentHits[pos++];
                ++documentsReturned;
                return new ElasticSearchDocumentHit(hit);
            }

        };

        Iterable<ISearchServerDocument> documentIterable = () -> documentIt;
        return StreamSupport.stream(documentIterable.spliterator(), false);
    }

    @Override
    public long getNumFound() {
        if (searchServerNotReachable)
            return 0;
        if (null != countResponse)
            return countResponse.getCount();
        if (null != response)
            return response.getHits().getTotalHits().value;

        return 0;
    }

    @Override
    public String getNumFoundRelation() {
        if (null != countResponse)
            return TotalHits.Relation.EQUAL_TO.name();
        if (null != response)
            return response.getHits().getTotalHits().relation.name();
        return null;
    }

    @Override
    public String getQueryErrorType() {
        return queryError != null ? queryError.name() : null;
    }

    @Override
    public long getNumSuggestions() {
        if (null != suggest) {
            Suggestion<? extends org.elasticsearch.search.suggest.Suggest.Suggestion.Entry<? extends Option>> suggestion = suggest
                    .getSuggestion("");
            if (suggestion != null) {
                if (suggestion.getEntries() != null) {
                    return suggestion.getEntries().get(0).getOptions().size();
                }
            }
        }
        return 0;
    }

    @Override
    public List<ISearchServerDocument> getSuggestionResults() {
        List<ISearchServerDocument> documents = new ArrayList<>();
        final List<? extends org.elasticsearch.search.suggest.Suggest.Suggestion.Entry<? extends Option>> entries = suggest
                .getSuggestion("").getEntries();
        for (final org.elasticsearch.search.suggest.Suggest.Suggestion.Entry<? extends Option> entry : entries) {
            for (final Option option : entry.getOptions()) {
                CompletionSuggestion.Entry.Option completionOption = (CompletionSuggestion.Entry.Option) option;
                ISearchServerDocument document = new ISearchServerDocument() {

                    @Override
                    public Optional<List<Object>> getFieldValues(String fieldName) {
                        return getFieldPayload(fieldName);
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public <V> Optional<V> getFieldValue(String fieldName) {
                        // the field "text" is the special option field
                        // that holds the actually suggested text
                        if (fieldName.equals("text"))
                            return Optional
                                    .ofNullable((V) (option.getText() != null ? option.getText().toString() : null));
                        return getFieldPayload(fieldName);
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public <V> Optional<V> get(String fieldName) {
                        return getFieldValue(fieldName);
                    }

                    @Override
                    public <V> Optional<V> getFieldPayload(String fieldName) {
                        return completionOption.getHit().field(fieldName).getValue();
                    }

                    @Override
                    public float getScore() {
                        return option.getScore();
                    }

                };
                documents.add(document);
            }
        }

        return documents;
    }

    @Override
    public boolean isSuggestionSearchResponse() {
        return isSuggestionSearchResponse;
    }

    @Override
    public void setSuggestionSearchResponse(boolean isSuggestionSearchResponse) {
        this.isSuggestionSearchResponse = isSuggestionSearchResponse;
    }

    @Override
    public boolean isCountResponse() {
        return countResponse != null;
    }

    public QueryError getQueryError() {
        return queryError;
    }

    public void setQueryError(QueryError queryError) {
        this.queryError = queryError;

    }

    public String getQueryErrorMessage() {
        return queryErrorMessage;
    }

    public void setQueryErrorMessage(String queryErrorMessage) {
        this.queryErrorMessage = queryErrorMessage;
    }

    public boolean hasQueryError() {
        return this.queryError != null;
    }

}
