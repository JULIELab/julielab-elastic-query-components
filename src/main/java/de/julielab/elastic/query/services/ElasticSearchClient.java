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

	public ElasticSearchClient(Logger log, String clusterName, String[] hosts, int[] ports) {
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
				client = new RestHighLevelClient(RestClient.builder(httpHosts));
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