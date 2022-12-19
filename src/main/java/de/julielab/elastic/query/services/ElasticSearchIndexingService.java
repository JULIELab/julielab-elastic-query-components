package de.julielab.elastic.query.services;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.slf4j.Logger;

public class ElasticSearchIndexingService implements IIndexingService {

    private Logger log;
    private RestHighLevelClient client;

    public ElasticSearchIndexingService(Logger log, ISearchClientProvider searchServerProvider) {
        this.log = log;
        ElasticSearchClient semedicoSearchClient = (ElasticSearchClient) searchServerProvider.getSearchClient();
        client = semedicoSearchClient.getRestHighLevelClient();
    }

    @Override
    public void indexDocuments(String index, Iterator<Map<String, Object>> documentIterator) {
        try {
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
                    br.add(ir);
                    batchCount++;
                    overall++;
                }
                BulkResponse response = client.bulk(br, RequestOptions.DEFAULT);
                if (response.hasFailures()) {
                    log.error("Error while indexing: {}", response.buildFailureMessage());
                }
                if (overall % 1000000 == 0)
                    log.info("{} documents indexed.", overall);
            }
        } catch (IOException e) {
            log.error("Could not index documents to index {}", index, e);
        }

    }

    @Override
    public void indexDocuments(String index, List<Map<String, Object>> documents) {
        log.info("Beginning to add {} documents to the index \"{}\".", documents.size(), index);
        indexDocuments(index, documents.iterator());
    }

    @Override
    public void clearIndex(String index) {
        try {
            log.info("Clearing index {}", index);
            BulkByScrollResponse response = client.deleteByQuery(new DeleteByQueryRequest(index).setQuery(QueryBuilders.matchAllQuery()), RequestOptions.DEFAULT);

            log.info("Number of deleted documents from index {}: {}.", index, response.getDeleted());
        } catch (IOException e) {
            log.error("Could not clear index {}", index, e);
        }

    }

    @Override
    public void commit(String index) {
        try {
            log.info("Refreshing index to make documents immediately accessible.");
            client.indices().refresh(new RefreshRequest(index), RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("Could not refresh index {}", index, e);
        }
    }

}
