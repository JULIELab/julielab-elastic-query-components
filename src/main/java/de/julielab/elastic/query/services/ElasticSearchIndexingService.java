package de.julielab.elastic.query.services;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.slf4j.Logger;

public class ElasticSearchIndexingService implements IIndexingService {

	private Logger log;
	private Client client;

	public ElasticSearchIndexingService(Logger log, ISearchClientProvider searchServerProvider) {
		this.log = log;
		ElasticSearchClient semedicoSearchClient = (ElasticSearchClient) searchServerProvider.getSearchClient();
		client = semedicoSearchClient.getClient();
	}

	@Override
	public void indexDocuments(String index, String type, Iterator<Map<String, Object>> documentIterator) {
		// if (indexType != IIndexInformationService.Index.SUGGESTIONS)
		// throw new IllegalArgumentException("Only suggestions are supported");

		log.info("Indexing documents from iterator into index \"{}\".", index);

		int overall = 0;
		while (documentIterator.hasNext()) {
			int batchCount = 0;
			BulkRequest br = new BulkRequest();
			while (documentIterator.hasNext() && batchCount < 1000) {
				Map<String, Object> doc = documentIterator.next();
				IndexRequest ir = new IndexRequest(index);
				if (doc.get("_id") != null) {
					ir.id((String) doc.get("_id"));
					// in ElasticSearch, the document must not contain the _id
					// field itself
					doc.remove("_id");
				}
				ir.source(doc);
				ir.type(type);
				br.add(ir);
				batchCount++;
				overall++;
			}
			ActionFuture<BulkResponse> future = client.bulk(br);
			try {
				BulkResponse response = future.get();
				if (response.hasFailures()) {
					log.error("Error while indexing: {}", response.buildFailureMessage());
				}
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			if (overall % 1000000 == 0)
				log.info("{} documents indexed.", overall);
		}

	}

	@Override
	public void indexDocuments(String index, String type, List<Map<String, Object>> documents) {
		log.info("Beginning to add {} documents to the index \"{}\".", documents.size(), index);
		indexDocuments(index, type, documents.iterator());
	}

	@Override
	public void clearIndex(String index) {
		log.info("Clearing index {}", index);
		BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
				.filter(QueryBuilders.matchAllQuery()).source(index).get();

		log.info("Deleting by all query deleted {} documents.", response.getDeleted());

	}

	@Override
	public void commit(String index) {
		log.info("Refreshing index to make documents immediately accessible.");
		client.admin().indices().prepareRefresh(index).execute().actionGet();
	}

}
