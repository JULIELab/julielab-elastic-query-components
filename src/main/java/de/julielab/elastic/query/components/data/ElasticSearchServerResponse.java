package de.julielab.elastic.query.components.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.slf4j.Logger;

import de.julielab.elastic.query.components.data.aggregation.AggregationCommand;
import de.julielab.elastic.query.components.data.aggregation.IAggregationResult;
import de.julielab.elastic.query.components.data.aggregation.MaxAggregation;
import de.julielab.elastic.query.components.data.aggregation.MaxAggregationResult;
import de.julielab.elastic.query.components.data.aggregation.SignificantTermsAggregation;
import de.julielab.elastic.query.components.data.aggregation.SignificantTermsAggregationResult;
import de.julielab.elastic.query.components.data.aggregation.SignificantTermsAggregationUnit;
import de.julielab.elastic.query.components.data.aggregation.TermsAggregation;
import de.julielab.elastic.query.components.data.aggregation.TermsAggregationResult;
import de.julielab.elastic.query.components.data.aggregation.TermsAggregationUnit;
import de.julielab.elastic.query.components.data.aggregation.TopHitsAggregation;
import de.julielab.elastic.query.components.data.aggregation.TopHitsAggregationResult;
import de.julielab.elastic.query.services.ISearchServerResponse;
import de.julielab.elastic.query.util.TermCountCursor;

public class ElasticSearchServerResponse implements ISearchServerResponse {

	private SearchResponse response;
	private List<FacetCommand> facetCmds;
	private boolean searchServerNotReachable;
	private boolean isSuggestionSearchResponse;
	private Logger log;
	private Suggest suggest;
	private Map<String, Aggregation> aggregationsByName;
	private QueryError queryError;
	private Client client;

	public ElasticSearchServerResponse(Logger log, SearchResponse response, List<FacetCommand> facetCmds,
			Client client) {
		this.log = log;
		this.response = response;
		this.facetCmds = facetCmds;
		this.client = client;
		this.suggest = response.getSuggest();
		if (null != response.getAggregations())
			this.aggregationsByName = response.getAggregations().asMap();
	}

	public ElasticSearchServerResponse(Logger log) {
		this(log, null, null, null);
	}

	public IAggregationResult getAggregationResult(AggregationCommand aggCmd) {
		String aggName = aggCmd.name;
		if (null == aggregationsByName || null == aggregationsByName.get(aggName)) {
			log.warn("No aggregation result with name \"{}\" was found in ElasticSearch's response.", aggName);
			return null;
		}
		Aggregation aggregation = aggregationsByName.get(aggName);

		return buildAggregationResult(aggCmd, aggregation);
	}

	private IAggregationResult buildAggregationResult(AggregationCommand aggCmd, Aggregation aggregation) {
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
					AggregationCommand subAggCmd = semedicoTermsAggregation.getSubaggregation(esSubAggName);
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
			long totalHits = esTopHitsAggregation.getHits().getTotalHits();
			topHitsResult.setTotalHits(totalHits);
			SearchHits searchHits = esTopHitsAggregation.getHits();
			for (int i = 0; i < searchHits.getHits().length; i++) {
				final SearchHit searchHit = searchHits.getHits()[i];
				// For the ElasticSearch TopHitsAggregation we don't get the
				// stored fields back but the _source field
				// (see ElasticSearch documentation).
				// final Map<String, Object> source = searchHit.getSource();
				final Map<String, SearchHitField> source = searchHit.getFields();

				ISearchServerDocument serverDoc = new ISearchServerDocument() {

					@Override
					public Optional<List<Object>> getFieldValues(String fieldName) {
						return Optional.ofNullable((List<Object>) source.get(fieldName).getValues());
					}

					@SuppressWarnings("unchecked")
					@Override
					public <V> Optional<V> getFieldValue(String fieldName) {
						return Optional.ofNullable((V) source.get(fieldName).getValue());
					}

					@SuppressWarnings("unchecked")
					@Override
					public <V> Optional<V> get(String fieldName) {
						return Optional.ofNullable(((V) source.get(fieldName).getValue()));
					}

					@Override
					public String getId() {
						return searchHit.getId();
					}

					@Override
					public String getIndexType() {
						return searchHit.getType();
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
	public List<IFacetField> getFacetFields() {
		List<IFacetField> facetFields = Collections.emptyList();
		Aggregations aggregations = response.getAggregations();
		if (null == aggregations && !searchServerNotReachable)
			throw new IllegalStateException(
					"ElasticSearch did not return any facet counts, but they were demanded by the application.");
		if (null != aggregations) {
			// a map from the name of an aggregation to the aggregation itself
			Map<String, Aggregation> aggMap = aggregations.getAsMap();
			// List<Facet> facets = esFacets.facets();
			facetFields = new ArrayList<>(aggMap.size());
			for (FacetCommand fc : facetCmds) {
				String facetName = fc.name;
				if (StringUtils.isBlank(facetName) || facetName.startsWith("null")) {
					throw new IllegalArgumentException("The facet command \"" + fc
							+ "\" has no name. Thus, it is not clear how this facet command is referenced in the ElasticSearch response (logical facet name).");
				}
				facetFields.add(new ElasticSearchConversionFacetField(facetName, aggMap));
			}
		}
		return facetFields;
	}

	@Override
	public Stream<ISearchServerDocument> getDocumentResults() {
		if (searchServerNotReachable) {
			log.debug("Not returning any document results because the server was not reachable.");
			return Stream.empty();
		}

		Iterator<ISearchServerDocument> documentIt = new Iterator<ISearchServerDocument>() {

			private int pos = 0;
			private SearchHit[] currentHits = response.getHits().getHits();

			@Override
			public boolean hasNext() {
				if (pos < currentHits.length) {
					log.trace("There are more documents in the current response.");
					return true;
				} else if (!StringUtils.isBlank(response.getScrollId())) {
					log.debug(
							"No more documents present in the current response but got scroll ID {}. Querying next batch.",
							response.getScrollId());
					SearchResponse scrollResponse = client.prepareSearchScroll(response.getScrollId())
							.setScroll(TimeValue.timeValueMinutes(5)).execute().actionGet();
					currentHits = scrollResponse.getHits().getHits();
					log.trace("Received {} new hits from scroll request.", currentHits.length);
					pos = 0;
					if (currentHits.length > 0)
						return true;
				}
				log.debug("No more hits returned from scrolling request.");
				pos = Integer.MAX_VALUE;
				if (response.getScrollId() != null) {
					log.debug("Closing the scroll with ID {}", response.getScrollId());
					client.prepareClearScroll().addScrollId(response.getScrollId()).execute();
					response.scrollId(null);
				}
				return false;
			}

			@Override
			public ISearchServerDocument next() {
				if (!hasNext())
					return null;
				log.trace("Returning next document at position {} of the current scroll batch.", pos);
				SearchHit hit = currentHits[pos++];
				// get Highlighting, if any
				Map<String, HighlightField> esHLs = hit.getHighlightFields();
				// this map will for each highlighted field name contain the
				// list of highlights
				Map<String, List<String>> fieldHLs = new HashMap<>(esHLs.size());
				for (Entry<String, HighlightField> esFieldHLs : esHLs.entrySet()) {
					String fieldName = esFieldHLs.getKey();
					HighlightField hf = esFieldHLs.getValue();

					List<String> hLFragments = new ArrayList<>(hf.fragments().length);
					for (Text esHLFragments : hf.getFragments())
						hLFragments.add(esHLFragments.string());
					fieldHLs.put(fieldName, hLFragments);
				}

				ISearchServerDocument document = new ElasticSearchDocumentHit(hit);
				return document;
			}

		};

		Iterable<ISearchServerDocument> documentIterable = () -> documentIt;
		return StreamSupport.stream(documentIterable.spliterator(), false);
	}

	@Override
	public Map<String, Map<String, List<String>>> getHighlighting() {
		SearchHits hits = response.getHits();
		Map<String, Map<String, List<String>>> highlighting = new HashMap<>(hits.hits().length);

		for (SearchHit hit : hits) {
			String docId = hit.getId();
			Map<String, HighlightField> esHLs = hit.getHighlightFields();
			Map<String, List<String>> fieldHLs = new HashMap<>(esHLs.size());

			for (Entry<String, HighlightField> esFieldHLs : esHLs.entrySet()) {
				String fieldName = esFieldHLs.getKey();
				HighlightField hf = esFieldHLs.getValue();

				List<String> hLFragments = new ArrayList<>(hf.fragments().length);
				for (Text esHLFragments : hf.getFragments())
					hLFragments.add(esHLFragments.string());
				fieldHLs.put(fieldName, hLFragments);
			}

			highlighting.put(docId, fieldHLs);
		}
		return highlighting;
	}

	@Override
	public long getNumFound() {
		if (searchServerNotReachable)
			return 0;
		if (null != response)
			return response.getHits().getTotalHits();

		return 0;
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

	/**
	 * This class takes all aggregations (all facets that were requested) and
	 * just pulls the one with the correct name (see constructor arguments) out
	 * of those. This is then the actual "facet field".
	 * 
	 * @author faessler
	 * 
	 */
	private class ElasticSearchConversionFacetField implements IFacetField {

		private final String name;
		private Map<String, Aggregation> aggMap;

		public ElasticSearchConversionFacetField(String name, Map<String, Aggregation> aggMap) {
			this.name = name;
			this.aggMap = aggMap;
		}

		@Override
		public TermCountCursor getFacetValues() {
			final Map<FacetType, Terms> facetMap = new HashMap<>();
			for (String aggName : aggMap.keySet()) {
				try {
					if (aggName.startsWith(name)) {
						String typeString = aggName.substring(name.length());
						FacetType type = FacetType.valueOf(typeString);
						facetMap.put(type, (Terms) aggMap.get(aggName));
					}
				} catch (IllegalArgumentException e) {
					// Do nothing, this can happen because our facet names are
					// not prefix-free. E.g. "facetTermsfid1"
					// will
					// hit when the actual field name is "facetTermsfid13count"
					// and we will get an error because the
					// enum constant '3count' does not exist.
				}
			}
			// We just need a facet to give us the term names. All facets have
			// the
			// same names in the same order (everything else would be an error),
			// only their counts are different according to their FacetType
			// (count
			// vs. document frequency for example).
			final Terms referenceFacet = facetMap.values().iterator().next();

			return new TermCountCursor() {

				private long numElements = referenceFacet.getBuckets().size();
				private int pos = -1;

				@Override
				public boolean forwardCursor() {
					pos++;
					return isValid();
				}

				@Override
				public String getName() {
					if (isValid())
						return (String) referenceFacet.getBuckets().get(pos).getKey();
					return null;
				}

				@Override
				public Number getFacetCount(FacetType type) {
					if (isValid())
						return referenceFacet.getBuckets().get(pos).getDocCount();
					return null;
					// TermsFacet tf = facetMap.get(type);
					// return tf.getEntries().get(pos).getCount();
				}

				@Override
				public long size() {
					return numElements;
				}

				@Override
				public boolean isValid() {
					return pos > -1 && pos < numElements;
				}

				@Override
				public void reset() {
					pos = -1;
				}

			};
		}

		@Override
		public String getName() {
			return name;
		}

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
						return Optional.ofNullable((V) getFieldValue(fieldName).get());
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

	public void setQueryError(QueryError queryError) {
		this.queryError = queryError;

	}

	public QueryError getQueryError() {
		return queryError;
	}
}
