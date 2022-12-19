package de.julielab.elastic.query.services;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.stream.IntStream;

public class ElasticSearchClient implements ISearchClient {
	private String clusterName;
	private String[] hosts;
	private int[] ports;
	private RestHighLevelClient client;
	private Logger log;
	private int socketTimeout;

	public ElasticSearchClient(Logger log, String clusterName, String[] hosts, int[] ports, int socketTimeout) {
		this.socketTimeout = socketTimeout;
		if (hosts.length != ports.length)
			throw new IllegalArgumentException("The number of hosts and ports must be equal.");
		this.log = log;
		this.clusterName = clusterName;
		this.hosts = hosts;
		this.ports = ports;
	}

	@SuppressWarnings("resource")
	public RestHighLevelClient getRestHighLevelClient() {
			if (null == client) {
				log.info("Connecting to a ElasticSearch cluster {} via socket connection \"{}:{}\".",
						new Object[] { clusterName, hosts, ports});
				HttpHost[] httpHosts = IntStream.range(0, hosts.length).mapToObj(i -> new HttpHost(hosts[i], ports[i], "http")).toArray(HttpHost[]::new);
				client = new RestHighLevelClient(RestClient.builder(httpHosts)
						// https://discuss.elastic.co/t/how-to-avoid-30-000ms-timeout-during-reindexing/231370
						.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setSocketTimeout(socketTimeout)));
			}
			return client;
	}

	@Override
	public void shutdown() {
		try {
			if (null != client)
				client.close();
		} catch (IOException e) {
			log.error("Could not close ElasticSearch client", e);
		}

	}
}