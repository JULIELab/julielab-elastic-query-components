package de.julielab.elastic.query.services;

import org.elasticsearch.client.Client;

public interface ISearchClient {
	void shutdown();
	Client getClient();
}
