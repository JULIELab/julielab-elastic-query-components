package de.julielab.elastic.query.services;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.slf4j.Logger;

import de.julielab.elastic.query.services.ISearchClient;

public class ElasticSearchClient implements ISearchClient {
	private String clusterName;
	private String host;
	private Node node;
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
		if (null == node) {
			log.info("Starting up a node via Zen Discovery to ElasticSearch cluster names \"{}\".", clusterName);
			node = nodeBuilder().client(true).clusterName(clusterName).node();
		}
		return node;
	}

	public Client getNodeClient() {
		return getNode().client();
	}

	public Client getTransportClient() {
		try {
			if (null == transportClient) {
				log.info("Connecting to a ElasticSearch cluster {} via socket connection \"{}:{}\".", new Object[]{clusterName, host, port});

				Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName)
						.build();
				// since the DeleteByQuery functionality is a plugin, we
				// also have to register it here; it won't work
				// otherwise and throw an NPE
				transportClient = TransportClient.builder().addPlugin(DeleteByQueryPlugin.class).settings(settings).build();
				transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
			}
			return transportClient;
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public void shutdown() {
		if (null != node)
			node.close();
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