package de.julielab.elastic.query.components.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.commons.lang.NotImplementedException;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;

public class ElasticSearchDocumentHit implements ISearchServerDocument {

	private SearchHit hit;
	private Map<String, List<String>> semedicoFieldHLs;
	Map<String, List<ISearchServerDocument>> innerHits;

	public ElasticSearchDocumentHit(SearchHit hit) {
		this.hit = hit;
	}

	@Override
	public List<Object> getFieldValues(String fieldName) {
		SearchHitField field = hit.field(fieldName);
		if (null == field)
			return null;
		return field.getValues();
	}

	@Override
	public <V> Optional<V> getFieldValue(String fieldName) {
		SearchHitField field = hit.field(fieldName);
		return Optional.ofNullable(field.getValue());
	}

	@Override
	public <V> V get(String fieldName) {
		SearchHitField field = hit.field(fieldName);
		if (null == field)
			return null;
		return field.getValue();
	}

	@Override
	public <V> V getFieldPayload(String fieldName) {
		throw new NotImplementedException();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Map<String, SearchHitField> fields = hit.getFields();
		for (Entry<String, SearchHitField> entry : fields.entrySet()) {
			sb.append(entry.getKey());
			sb.append(": ");
			sb.append(entry.getValue().getValue().toString());
			sb.append("\n");
		}
		return sb.toString();
	}

	@Override
	public Map<String, List<ISearchServerDocument>> getInnerHits() {
		if (null == hit.getInnerHits())
			return Collections.emptyMap();
		if (null == innerHits) {
			innerHits = new HashMap<>();

			Map<String, SearchHits> esInnerHits = hit.getInnerHits();
			for (String nestedFieldName : esInnerHits.keySet()) {
				SearchHits searchHits = esInnerHits.get(nestedFieldName);
				List<ISearchServerDocument> documents = new ArrayList<>();
				for (int i = 0; i < searchHits.getHits().length; i++) {
					SearchHit esHit = searchHits.getHits()[i];
					ElasticSearchDocumentHit document = new ElasticSearchDocumentHit(esHit);
					documents.add(document);
				}
				innerHits.put(nestedFieldName, documents);
			}
		}
		return innerHits;
	}

	@Override
	public Map<String, List<String>> getHighlights() {
		Map<String, HighlightField> esHLs = hit.getHighlightFields();
		if (null == semedicoFieldHLs) {
			semedicoFieldHLs = new HashMap<>(esHLs.size());

			for (Entry<String, HighlightField> esFieldHLs : esHLs.entrySet()) {
				String fieldName = esFieldHLs.getKey();
				HighlightField hf = esFieldHLs.getValue();

				List<String> semedicoHLFragments = new ArrayList<>(hf.fragments().length);
				for (Text esHLFragments : hf.getFragments())
					semedicoHLFragments.add(esHLFragments.string());
				semedicoFieldHLs.put(fieldName, semedicoHLFragments);
			}
		}
		return semedicoFieldHLs;
	}

	@Override
	public String getId() {
		return hit.getId();
	}

	@Override
	public String getIndexType() {
		return hit.getType();
	}

	@Override
	public float getScore() {
		return hit.getScore();
	}

}
