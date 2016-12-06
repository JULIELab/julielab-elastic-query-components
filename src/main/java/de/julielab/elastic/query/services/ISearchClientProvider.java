package de.julielab.elastic.query.services;



public interface ISearchClientProvider {
	
	ISearchClient getSearchClient();
	
//	<T> T getSearchServer(IndexTypes indexType);
//
//	<T> T getSearchServer();
}
