package de.julielab.elastic.query.components.data;

import org.apache.commons.lang.NotImplementedException;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.util.*;
import java.util.Map.Entry;

public class ElasticSearchDocumentHit implements ISearchServerDocument {

	private SearchHit hit;
	private Map<String, List<String>> fieldHightlights;
	private Map<String, List<ISearchServerDocument>> innerHits;

	ElasticSearchDocumentHit(SearchHit hit) {
		this.hit = hit;
	}

	@Override
	public Optional<List<Object>> getFieldValues(String fieldName) {
		DocumentField field = hit.field(fieldName);
		if (null == field)
			return Optional.empty();
		return Optional.ofNullable(field.getValues());
	}

	@Override
	public <V> Optional<V> getFieldValue(String fieldName) {
		DocumentField field = hit.field(fieldName);
		return Optional.ofNullable(field).map(DocumentField::getValue);
	}

	@Override
	public <V> Optional<V> get(String fieldName) {
		DocumentField field = hit.field(fieldName);
		if (null == field)
			return Optional.empty();
		return Optional.ofNullable(field.getValue());
	}

	@Override
	public <V> Optional<V> getFieldPayload(String fieldName) {
		throw new NotImplementedException();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Map<String, DocumentField> fields = hit.getFields();
		for (Entry<String, DocumentField> entry : fields.entrySet()) {
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
		if (null == fieldHightlights) {
			fieldHightlights = new HashMap<>(esHLs.size());

			for (Entry<String, HighlightField> esFieldHLs : esHLs.entrySet()) {
				String fieldName = esFieldHLs.getKey();
				HighlightField hf = esFieldHLs.getValue();

				List<String> semedicoHLFragments = new ArrayList<>(hf.fragments().length);
				for (Text esHLFragments : hf.getFragments())
					semedicoHLFragments.add(esHLFragments.string());
				fieldHightlights.put(fieldName, semedicoHLFragments);
			}
		}
		return fieldHightlights;
	}

	@Override
	public String getId() {
		return hit.getId();
	}

	@Override
	public float getScore() {
		return hit.getScore();
	}

}
