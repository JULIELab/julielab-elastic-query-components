package de.julielab.elastic.query.services;

import org.elasticsearch.client.RestHighLevelClient;

public interface ISearchClient {
	void shutdown();
	RestHighLevelClient getRestHighLevelClient();
}
