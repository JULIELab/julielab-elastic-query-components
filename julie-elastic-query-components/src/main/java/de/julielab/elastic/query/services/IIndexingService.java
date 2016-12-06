package de.julielab.elastic.query.services;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface IIndexingService {
	void indexDocuments(String index, String type, Iterator<Map<String, Object>> documentIterator);

	void indexDocuments(String index, String type, List<Map<String, Object>> documents);

	void clearIndex(String index);

	void commit(String index);
}
