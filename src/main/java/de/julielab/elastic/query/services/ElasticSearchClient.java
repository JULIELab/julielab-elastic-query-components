package de.julielab.elastic.query.services;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;

public class ElasticSearchClient implements ISearchClient {
	private String clusterName;
	private String host;
	private int port;
	private TransportClient transportClient;
	private Logger log;

	public ElasticSearchClient(Logger log, String clusterName, String host, int port) {
		this.log = log;
		this.clusterName = clusterName;
		this.host = host;
		this.port = port;
	}

	public Node getNode() {
		throw new NotImplementedException();
	}

	public Client getNodeClient() {
		return getNode().client();
	}

	@SuppressWarnings("resource")
	public Client getTransportClient() {
		try {
			if (null == transportClient) {
				log.info("Connecting to a ElasticSearch cluster {} via socket connection \"{}:{}\".",
						new Object[] { clusterName, host, port });

				Settings settings = Settings.builder().put("cluster.name", clusterName).build();
				transportClient = new PreBuiltTransportClient(settings)
						.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
			}
			return transportClient;
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public void shutdown() {
		if (null != transportClient)
			transportClient.close();

	}

	public Client getClient() {
		if (!StringUtils.isBlank(host) && port != -1)
			return getTransportClient();
		else if (!StringUtils.isBlank(clusterName))
			return getNodeClient();
		else
			throw new IllegalStateException(
					"Neither an ElasticSearch cluster name nor host and port are delivered. Unable to create ElasticSearch client.");
	}
}