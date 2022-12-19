package de.julielab.elastic.query.services;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface IIndexingService {
    void indexDocuments(String index, Iterator<Map<String, Object>> documentIterator);

    void indexDocuments(String index, List<Map<String, Object>> documents);

    void clearIndex(String index);

    void commit(String index);
}
