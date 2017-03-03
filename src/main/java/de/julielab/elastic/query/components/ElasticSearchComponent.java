package de.julielab.elastic.query.components;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder.Type;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.fieldvaluefactor.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.support.QueryInnerHitBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder.Field;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import de.julielab.elastic.query.components.data.ElasticSearchServerResponse;
import de.julielab.elastic.query.components.data.FacetCommand;
import de.julielab.elastic.query.components.data.HighlightCommand;
import de.julielab.elastic.query.components.data.HighlightCommand.HlField;
import de.julielab.elastic.query.components.data.IFacetField.FacetType;
import de.julielab.elastic.query.components.data.QueryError;
import de.julielab.elastic.query.components.data.SearchCarrier;
import de.julielab.elastic.query.components.data.SearchServerCommand;
import de.julielab.elastic.query.components.data.SortCommand;
import de.julielab.elastic.query.components.data.aggregation.AggregationCommand;
import de.julielab.elastic.query.components.data.aggregation.AggregationCommand.OrderCommand;
import de.julielab.elastic.query.components.data.aggregation.MaxAggregation;
import de.julielab.elastic.query.components.data.aggregation.SignificantTermsAggregation;
import de.julielab.elastic.query.components.data.aggregation.TermsAggregation;
import de.julielab.elastic.query.components.data.aggregation.TopHitsAggregation;
import de.julielab.elastic.query.components.data.query.BoolClause;
import de.julielab.elastic.query.components.data.query.BoolQuery;
import de.julielab.elastic.query.components.data.query.ConstantScoreQuery;
import de.julielab.elastic.query.components.data.query.FunctionScoreQuery;
import de.julielab.elastic.query.components.data.query.FunctionScoreQuery.BoostMode;
import de.julielab.elastic.query.components.data.query.FunctionScoreQuery.FieldValueFactor;
import de.julielab.elastic.query.components.data.query.LuceneSyntaxQuery;
import de.julielab.elastic.query.components.data.query.MatchAllQuery;
import de.julielab.elastic.query.components.data.query.MatchPhraseQuery;
import de.julielab.elastic.query.components.data.query.MatchQuery;
import de.julielab.elastic.query.components.data.query.MultiMatchQuery;
import de.julielab.elastic.query.components.data.query.NestedQuery;
import de.julielab.elastic.query.components.data.query.SearchServerQuery;
import de.julielab.elastic.query.components.data.query.TermQuery;
import de.julielab.elastic.query.components.data.query.TermsQuery;
import de.julielab.elastic.query.components.data.query.WildcardQuery;
import de.julielab.elastic.query.services.ISearchClientProvider;

public class ElasticSearchComponent extends AbstractSearchComponent implements ISearchServerComponent {

	// The following highlighting-defaults are taken from
	// http://www.elasticsearch.org/guide/reference/api/search/highlighting/
	// Default size of highlighting fragments
	private static final int DEFAULT_FRAGSIZE = 100;
	private static final int DEFAULT_NUMBER_FRAGS = 5;

	private static final String SEMEDICO_DEFAULT_SCRIPT_LANG = "groovy";
	private Logger log;
	private Client client;
	private static final Function<String, String> encloseTermsFunction = new Function<String, String>() {

		@Override
		public String apply(String input) {
			StringBuilder sb = new StringBuilder();
			sb.append("(");
			sb.append(input);
			sb.append(")");
			return sb.toString();
		}

	};

	public ElasticSearchComponent(Logger log, ISearchClientProvider searchClientProvider) {
		this.log = log;
		client = searchClientProvider.getSearchClient().getClient();
	}

	@Override
	public boolean processSearch(SearchCarrier searchCarrier) {
		StopWatch w = new StopWatch();
		w.start();
		List<SearchServerCommand> serverCmds = searchCarrier.serverCmds;
		if (null == serverCmds)
			throw new IllegalArgumentException("A " + SearchServerCommand.class.getName()
					+ " is required for an ElasticSearch search, but none is present.");

		// It could be that the search component occurs multiple times in a
		// search chain. But then, the last response(s) should have been
		// consumed by now.
		searchCarrier.serverResponses.clear();

		// One "Semedico search" may result in multiple search server commands,
		// e.g. suggestions where for each facet suggestions are searched or for
		// B-terms where there are multiple search nodes.
		// We should just take care that the results are ordered in a parallel
		// way to the server commands, see at the end of the method.
		List<SearchRequestBuilder> searchRequestBuilders = new ArrayList<>(serverCmds.size());
		List<SuggestRequestBuilder> suggestionBuilders = new ArrayList<>(serverCmds.size());
		log.debug("Number of searchs server commands: {}", serverCmds.size());
		for (int i = 0; i < serverCmds.size(); i++) {
			log.debug("Configuration ElasticSearch query for server command {}", i);

			SearchServerCommand serverCmd = serverCmds.get(i);

			if (null != serverCmd.query) {
				handleSearchRequest(searchRequestBuilders, serverCmd);
			}
			if (null != serverCmd.suggestionText) {
				handleSuggestionRequest(suggestionBuilders, serverCmd);
			}

		}

		// Send the query to the server
		try {
			if (!searchRequestBuilders.isEmpty()) {
				MultiSearchRequestBuilder multiSearch = client.prepareMultiSearch();
				for (SearchRequestBuilder srb : searchRequestBuilders)
					multiSearch.add(srb);
				MultiSearchResponse multiSearchResponse = multiSearch.execute().actionGet();
				Item[] responses = multiSearchResponse.getResponses();
				for (int i = 0; i < responses.length; i++) {
					Item item = responses[i];
					List<FacetCommand> facetCmds = serverCmds.get(i).facetCmds;
					SearchResponse response = item.getResponse();

					log.trace("Response from ElasticSearch: {}", response);

					ElasticSearchServerResponse serverRsp = new ElasticSearchServerResponse(log, response, facetCmds, client);
					searchCarrier.addSearchServerResponse(serverRsp);

					if (null == response) {
						serverRsp.setQueryError(QueryError.NO_RESPONSE);
					}
				}
			}
			if (!suggestionBuilders.isEmpty()) {
				for (SuggestRequestBuilder suggestBuilder : suggestionBuilders) {
					SuggestResponse suggestResponse = suggestBuilder.execute().actionGet();
					searchCarrier.addSearchServerResponse(new ElasticSearchServerResponse(suggestResponse));
				}
			}
			w.stop();
			log.debug("ElasticSearch process took {}ms ({}s)", w.getTime(), w.getTime() / 1000);
		} catch (NoNodeAvailableException e) {
			log.error("No ElasticSearch node available: {}", e.getMessage());
			ElasticSearchServerResponse serverRsp = new ElasticSearchServerResponse(log);
			serverRsp.setQueryError(QueryError.NO_NODE_AVAILABLE);
			// SemedicoSearchResult errorResult = new
			// SemedicoSearchResult(searchCarrier.searchCmd.semedicoQuery);
			// errorResult.errorMessage = "The search infrastructure currently
			// undergoes maintenance, please try again later."
			// + " If this error persists, please inform us about the issue."
			// + " We apologize for the inconvenience.";
			// searchCarrier.searchResult = errorResult;
			return true;
		}

		return false;
	}

	protected void handleSuggestionRequest(List<SuggestRequestBuilder> suggestBuilders, SearchServerCommand serverCmd) {
		CompletionSuggestionBuilder suggestionBuilder = new CompletionSuggestionBuilder("")
				.field(serverCmd.suggestionField).text(serverCmd.suggestionText).size(serverCmd.rows);
		if (null != serverCmd.suggestionCategories && serverCmd.suggestionCategories.size() > 0) {
			for (String context : serverCmd.suggestionCategories.keySet()) {
				for (String category : serverCmd.suggestionCategories.get(context))
					suggestionBuilder.addCategory(context, category);
			}
		}

		SuggestRequestBuilder suggestionRequestBuilder = client.prepareSuggest(serverCmd.index)
				.addSuggestion(suggestionBuilder);

		suggestBuilders.add(suggestionRequestBuilder);
		if (log.isDebugEnabled())
			log.debug("Suggesting on index {}. Created search query \"{}\".", serverCmd.index,
					client.prepareSearch(serverCmd.index).addSuggestion(suggestionBuilder));
	}

	protected void handleSearchRequest(List<SearchRequestBuilder> searchRequestBuilders,
			SearchServerCommand serverCmd) {
		if (null == serverCmd.fieldsToReturn)
			serverCmd.addField("*");

		if (serverCmd.index == null)
			throw new IllegalArgumentException("The search command does not define an index to search on.");
		SearchRequestBuilder srb = client.prepareSearch(serverCmd.index);
		if (serverCmd.indexTypes != null && !serverCmd.indexTypes.isEmpty())
			srb.setTypes(serverCmd.indexTypes.toArray(new String[serverCmd.indexTypes.size()]));

		srb.setFetchSource(serverCmd.fetchSource);
		// srb.setExplain(true);


		QueryBuilder queryBuilder = buildQuery(serverCmd.query);
		srb.setQuery(queryBuilder);

		if (null != serverCmd.fieldsToReturn)
			for (String field : serverCmd.fieldsToReturn) {
				srb.addField(field);
			}

		srb.setFrom(serverCmd.start);
		if (serverCmd.rows >= 0)
			srb.setSize(serverCmd.rows);
		else
			srb.setSize(0);

		if (null != serverCmd.aggregationCmds) {
			for (AggregationCommand aggCmd : serverCmd.aggregationCmds.values()) {
				log.debug("Adding top aggregation command {} to query.", aggCmd.name);
				AbstractAggregationBuilder aggregationBuilder = buildAggregation(aggCmd);
				srb.addAggregation(aggregationBuilder);
			}
		}

		log.debug("Number of facet commands: {}", serverCmd.facetCmds != null ? serverCmd.facetCmds.size() : 0);
		if (null != serverCmd.facetCmds) {
			for (FacetCommand fc : serverCmd.facetCmds) {
				if (fc.fields.size() == 0)
					throw new IllegalArgumentException("FacetCommand without fields to facet on occurred.");
				TermsBuilder fb = configureFacets(fc, FacetType.count);
				srb.addAggregation(fb);
			}
		}

		if (null != serverCmd.hlCmds && serverCmd.hlCmds.size() > 0) {
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
					srb.addHighlightedField(field);
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

	protected AbstractAggregationBuilder buildAggregation(AggregationCommand aggCmd) {
		if (TermsAggregation.class.equals(aggCmd.getClass())) {
			TermsAggregation termsAgg = (TermsAggregation) aggCmd;
			TermsBuilder termsBuilder = AggregationBuilders.terms(termsAgg.name).field(termsAgg.field);
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

			// Add sub aggregations
			if (null != termsAgg.subaggregations) {
				for (AggregationCommand subAggCmd : termsAgg.subaggregations.values()) {
					termsBuilder.subAggregation(buildAggregation(subAggCmd));
				}
			}
			return termsBuilder;
		}
		if (MaxAggregation.class.equals(aggCmd.getClass())) {
			MaxAggregation maxAgg = (MaxAggregation) aggCmd;
			MaxBuilder maxBuilder = AggregationBuilders.max(maxAgg.name);
			if (null != maxAgg.field)
				maxBuilder.field(maxAgg.field);
			if (null != maxAgg.script)
				maxBuilder.script(new Script(maxAgg.script, ScriptType.INLINE, SEMEDICO_DEFAULT_SCRIPT_LANG, null));
			return maxBuilder;
		}
		if (TopHitsAggregation.class.equals(aggCmd.getClass())) {
			TopHitsAggregation topHitsAgg = (TopHitsAggregation) aggCmd;
			TopHitsBuilder topHitsBuilder = AggregationBuilders.topHits(topHitsAgg.name);
			String[] includes = null;
			if (null != topHitsAgg.includeFields)
				includes = topHitsAgg.includeFields.toArray(new String[topHitsAgg.includeFields.size()]);
			String[] excludes = null;
			if (null != topHitsAgg.excludeFields)
				excludes = topHitsAgg.excludeFields.toArray(new String[topHitsAgg.excludeFields.size()]);
			if (null != includes || null != excludes)
				topHitsBuilder.setFetchSource(includes, excludes);
			if (topHitsAgg.size != null)
				topHitsBuilder.setSize(topHitsAgg.size);
			return topHitsBuilder;
		}
		if (SignificantTermsAggregation.class.equals(aggCmd.getClass())) {
			SignificantTermsAggregation sigAgg = (SignificantTermsAggregation) aggCmd;
			SignificantTermsBuilder esSigAgg = AggregationBuilders.significantTerms(sigAgg.name);
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
		} else {
			throw new IllegalArgumentException("Unhandled query type: " + searchServerQuery.getClass());
		}
		return queryBuilder;
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
		MatchQueryBuilder builder = QueryBuilders.matchPhraseQuery(matchPhraseQuery.field, matchPhraseQuery.phrase);
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
		esFunctionScoreQuery.boostMode(boostMode.name());

		return esFunctionScoreQuery;
	}

	private QueryBuilder buildNestedQuery(NestedQuery nestedQuery) {
		QueryBuilder esQuery = buildQuery(nestedQuery.query);
		NestedQueryBuilder nestedEsQuery = QueryBuilders.nestedQuery(nestedQuery.path, esQuery);
		if (null != nestedQuery.innerHits) {
			QueryInnerHitBuilder innerHitBuilder = new QueryInnerHitBuilder();
			innerHitBuilder.setFetchSource(nestedQuery.innerHits.fetchSource);
			for (String field : nestedQuery.innerHits.fields)
				innerHitBuilder.field(field);
			if (nestedQuery.innerHits.highlight != null) {
				HighlightCommand innerHl = nestedQuery.innerHits.highlight;
				for (HlField hlField : innerHl.fields) {
					Field esHlField = new Field(hlField.field);
					esHlField.fragmentSize(hlField.fragsize);
					esHlField.numOfFragments(hlField.fragnum);
					if (null != hlField.pre)
						esHlField.preTags(hlField.pre);
					if (null != hlField.post)
						esHlField.postTags(hlField.post);
					innerHitBuilder.addHighlightedField(esHlField);
				}
			}
			if (nestedQuery.innerHits.explain)
				innerHitBuilder.setExplain(nestedQuery.innerHits.explain);
			if (null != nestedQuery.innerHits.size)
				innerHitBuilder.setSize(nestedQuery.innerHits.size);
			nestedEsQuery.innerHit(innerHitBuilder);
			nestedEsQuery.scoreMode(nestedQuery.scoreMode.name());
		}
		return nestedEsQuery;
	}

	private QueryBuilder buildMatchQuery(MatchQuery matchQuery) {
		MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(matchQuery.field, matchQuery.query);
		switch (matchQuery.operator) {
		case "or":
		case "OR":
			matchQueryBuilder.operator(Operator.OR);
			break;
		case "and":
		case "AND":
			matchQueryBuilder.operator(Operator.AND);
			break;
		}
		if (null != matchQuery.analyzer)
			matchQueryBuilder.analyzer(matchQuery.analyzer);
		if (matchQuery.boost != 1f)
			matchQueryBuilder.boost(matchQuery.boost);
		return matchQueryBuilder;
	}

	protected QueryBuilder buildQueryStringQuery(LuceneSyntaxQuery luceneSyntaxQuery) {
		QueryBuilder queryBuilder;
		String queryString;
		queryString = luceneSyntaxQuery.query;
		// No fields given, so we assume a query string in Lucene syntax
		QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery(queryString);

		if (null != luceneSyntaxQuery && null != luceneSyntaxQuery.analyzer)
			queryStringQueryBuilder.analyzer(luceneSyntaxQuery.analyzer);
		// If we want to search for events using wildcards, the parameter
		// 'lowercaseExpandedTerms' has to be set
		// to
		// false or we won't get any results!!!
		queryStringQueryBuilder.lowercaseExpandedTerms(false);
		queryBuilder = queryStringQueryBuilder;
		return queryBuilder;
	}

	protected QueryBuilder buildMultiMatchQuery(MultiMatchQuery query) {
		log.debug("Building query string query.");
		MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(query.query);
		for (int i = 0; i < query.fields.size(); i++) {
			String field = query.fields.get(i);
			Float weight = null;
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

	private TermsBuilder configureFacets(FacetCommand fc, FacetType facetType) {
		if (fc.fields.size() > 1)
			throw new IllegalArgumentException(
					"The ElasticSearch component does not currently support multi-field facet counts.");
		TermsBuilder tb = AggregationBuilders.terms(fc.name + facetType).field(fc.fields.get(0))
				.size(fc.limit >= 0 ? fc.limit : Integer.MAX_VALUE);
		// TermsFacetBuilder tfb = FacetBuilders.termsFacet(fc.name + facetType)
		// .fields(fc.fields.toArray(new String[fc.fields.size()]))
		// .size(fc.limit >= 0 ? fc.limit : Integer.MAX_VALUE);
		// Term Sorting
		if (null != fc.sort) {
			Terms.Order order;
			// ComparatorType compType;
			switch (fc.sort) {
			case COUNT:
				order = Terms.Order.count(false);
				// compType = ComparatorType.COUNT;
				break;
			case TERM:
				order = Terms.Order.term(true);
				// compType = ComparatorType.TERM;
				break;
			case REVERSE_COUNT:
				order = Terms.Order.count(true);
				// compType = ComparatorType.REVERSE_COUNT;
				break;
			case REVERSE_TERM:
				order = Terms.Order.term(false);
				// compType = ComparatorType.REVERSE_TERM;
				break;
			default:
				throw new IllegalArgumentException("Unknown facet term sort order: " + fc.sort.name());
			}
			tb.order(order);
		}
		// Which terms to count
		if (null != fc.terms && fc.terms.size() > 0) {
			Collection<String> enclosedTerms = Collections2.transform(fc.terms, encloseTermsFunction);
			String includeTermRegexString = StringUtils.join(enclosedTerms, "|");
			if (!StringUtils.isEmpty(fc.filterExpression))
				includeTermRegexString += "(" + fc.filterExpression + ")";
			tb.include(includeTermRegexString);
		} else if (!StringUtils.isEmpty(fc.filterExpression)) {
			tb.include(fc.filterExpression);
		}
		return tb;
	}

}
