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
import de.julielab.elastic.query.services.ISearchServerResponse;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.MultiMatchQueryBuilder.Type;
import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder.Field;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.slf4j.Logger;

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

    private Client client;

    public ElasticSearchComponent(Logger log, ISearchClientProvider searchClientProvider) {
        super(log);
        log.info("Obtaining ElasticSearch client...");
        client = searchClientProvider.getSearchClient().getClient();
        log.info("ElasticSearch client retrieved.");
    }

    @Override
    public boolean processSearch(C elasticSearchCarrier) {
        StopWatch w = new StopWatch();
        w.start();
        List<SearchServerRequest> serverRequests = elasticSearchCarrier.getServerRequests();
        checkNotNull((Supplier<?>) () -> serverRequests, "Server requests");
        checkNotEmpty(serverRequests, "Server requests");
        stopIfError();

        // It could be that the search component occurs multiple times in a
        // search chain. But then, the last response(s) should have been
        // consumed by now.
        elasticSearchCarrier.clearSearchResponses();

        // One "Semedico search" may result in multiple search server commands,
        // e.g. suggestions where for each facet suggestions are searched or for
        // B-terms where there are multiple search nodes.
        // We should just take care that the results are ordered in a parallel
        // way to the server commands, see at the end of the method.
        List<SearchRequestBuilder> searchRequestBuilders = new ArrayList<>(serverRequests.size());
        List<SearchRequestBuilder> suggestionBuilders = new ArrayList<>(serverRequests.size());
        log.debug("Number of search server commands: {}", serverRequests.size());
        for (int i = 0; i < serverRequests.size(); i++) {
            log.debug("Configuring ElasticSearch query for server command {}", i);

            SearchServerRequest serverRequest = serverRequests.get(i);
            checkNotNull((Supplier<?>) () -> serverRequest, "Server request " + i);
            stopIfError();
            checkNotNull((Supplier<?>) () -> serverRequest.query, "Server request query for request " + i);
            stopIfError();

            if (null != serverRequest.query) {
                handleSearchRequest(searchRequestBuilders, serverRequest);
            }
            if (null != serverRequest.suggestionText) {
                handleSuggestionRequest(suggestionBuilders, serverRequest);
            }

        }

        // Send the query to the server
        try {
            if (searchRequestBuilders.size() == elasticSearchCarrier.getServerRequests().size()) {
                MultiSearchRequestBuilder multiSearch = client.prepareMultiSearch();
                for (SearchRequestBuilder srb : searchRequestBuilders)
                    multiSearch.add(srb);
                log.debug("Issueing {} search request as a multi search", searchRequestBuilders.size());
                MultiSearchResponse multiSearchResponse = multiSearch.execute().actionGet();
                Item[] responses = multiSearchResponse.getResponses();
                for (int i = 0; i < responses.length; i++) {
                    Item item = responses[i];
                    SearchResponse response = item.getResponse();

                    log.trace("Response from ElasticSearch: {}", response);

                    ElasticServerResponse serverRsp = new ElasticServerResponse(response, client);

                    if (null == response) {
                        serverRsp.setQueryError(QueryError.NO_RESPONSE);
                        serverRsp.setQueryErrorMessage(item.getFailureMessage());

                    }
                    elasticSearchCarrier.addSearchResponse(serverRsp);
                }
            } else {
                throw new IllegalStateException(
                        "There is at least one server request for which on ElasticSearch query could be created. This shouldn't happen.");
            }
            if (!suggestionBuilders.isEmpty()) {
                for (SearchRequestBuilder suggestBuilder : suggestionBuilders) {
                    SearchResponse suggestResponse = suggestBuilder.execute().actionGet();
                    elasticSearchCarrier.addSearchResponse(new ElasticServerResponse(suggestResponse, client));
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
        }

        return false;
    }

    protected void handleSuggestionRequest(List<SearchRequestBuilder> suggestBuilders, SearchServerRequest serverCmd) {
        SuggestBuilder suggestBuilder = new SuggestBuilder().addSuggestion("",
                SuggestBuilders.completionSuggestion(serverCmd.suggestionField).text(serverCmd.suggestionText));
        SearchRequestBuilder suggestionRequestBuilder = client.prepareSearch(serverCmd.index).suggest(suggestBuilder);

        suggestBuilders.add(suggestionRequestBuilder);
        if (log.isDebugEnabled())
            log.debug("Suggesting on index {}. Created search query \"{}\".", serverCmd.index,
                    suggestBuilder.toString());
    }

    protected void handleSearchRequest(List<SearchRequestBuilder> searchRequestBuilders,
                                       SearchServerRequest serverCmd) {
        if (null == serverCmd.fieldsToReturn)
            serverCmd.addField("*");

        if (serverCmd.index == null)
            throw new IllegalArgumentException("The search command does not define an index to search on.");
        SearchRequestBuilder srb = client.prepareSearch(serverCmd.index);
        if (serverCmd.indexTypes != null && !serverCmd.indexTypes.isEmpty())
            srb.setTypes(serverCmd.indexTypes.toArray(new String[serverCmd.indexTypes.size()]));

        log.trace("Searching on index {} and types {}", serverCmd.index, serverCmd.indexTypes);

        srb.setFetchSource(serverCmd.fetchSource);
        // srb.setExplain(true);

        if (serverCmd.downloadCompleteResults)
            srb.setScroll(TimeValue.timeValueMinutes(5));

        QueryBuilder queryBuilder = buildQuery(serverCmd.query);
        srb.setQuery(queryBuilder);

        if (null != serverCmd.fieldsToReturn)
            for (String field : serverCmd.fieldsToReturn) {
                srb.addStoredField(field);
            }

        srb.setFrom(serverCmd.start);
        if (serverCmd.rows >= 0)
            srb.setSize(serverCmd.rows);
        else
            srb.setSize(0);

        if (null != serverCmd.aggregationRequests) {
            for (AggregationRequest aggCmd : serverCmd.aggregationRequests.values()) {
                log.debug("Adding top aggregation command {} to query.", aggCmd.name);
                AbstractAggregationBuilder<?> aggregationBuilder = buildAggregation(aggCmd);
                if (aggregationBuilder != null)
                    srb.addAggregation(aggregationBuilder);
            }
        }

        if (null != serverCmd.hlCmds && serverCmd.hlCmds.size() > 0) {
            HighlightBuilder hb = new HighlightBuilder();
            srb.highlighter(hb);
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
                    // preTags.add(hlc.pre);
                    // postTags.add(hlc.post);
                    if (null != hlField.pre)
                        field.preTags(hlField.pre);
                    if (null != hlField.post)
                        field.postTags(hlField.post);
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
                srb.addSort(sortCmd.field, sort);
            }
        }

        if (null != serverCmd.postFilterQuery) {
            QueryBuilder postFilter = buildQuery(serverCmd.postFilterQuery);
            srb.setPostFilter(postFilter);
        }

        searchRequestBuilders.add(srb);

        log.debug("Searching on index {}. Created search query \"{}\".", serverCmd.index, srb.toString());
    }

    protected AbstractAggregationBuilder<?> buildAggregation(AggregationRequest aggCmd) {
        if (NoOpAggregation.class.equals(aggCmd.getClass()))
            return null;
        if (TermsAggregation.class.equals(aggCmd.getClass())) {
            TermsAggregation termsAgg = (TermsAggregation) aggCmd;

            TermsAggregationBuilder termsBuilder = AggregationBuilders.terms(termsAgg.name).field(termsAgg.field);
            List<Terms.Order> compoundOrder = new ArrayList<>();
            for (OrderCommand orderCmd : termsAgg.order) {
                Terms.Order order = null;
                boolean ascending = false;
                if (null != orderCmd && null != orderCmd.sortOrder)
                    ascending = orderCmd.sortOrder == OrderCommand.SortOrder.ASCENDING;
                if (null != orderCmd) {
                    switch (orderCmd.referenceType) {
                        case AGGREGATION_MULTIVALUE:
                            order = Terms.Order.aggregation(orderCmd.referenceName, orderCmd.metric.name(), ascending);
                            break;
                        case AGGREGATION_SINGLE_VALUE:
                            order = Terms.Order.aggregation(orderCmd.referenceName, ascending);
                            break;
                        case COUNT:
                            order = Terms.Order.count(ascending);
                            break;
                        case TERM:
                            order = Terms.Order.term(ascending);
                            break;
                    }
                    if (null != order)
                        compoundOrder.add(order);
                }
            }
            if (!compoundOrder.isEmpty())
                termsBuilder.order(Terms.Order.compound(compoundOrder));
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
        }
        else {
            throw new IllegalArgumentException("Unhandled query type: " + searchServerQuery.getClass());
        }
        return queryBuilder;
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
        log.trace("Building query string query.");
        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(query.query);
        for (int i = 0; i < query.fields.size(); i++) {
            String field = query.fields.get(i);
            Float weight;
            if (null != query.fieldWeights) {
                weight = query.fieldWeights.get(i);
                multiMatchQueryBuilder.field(field, weight);
            } else {
                multiMatchQueryBuilder.field(field);
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
