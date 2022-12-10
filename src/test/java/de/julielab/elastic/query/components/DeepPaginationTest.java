package de.julielab.elastic.query.components;

import de.julielab.elastic.query.components.data.ElasticSearchCarrier;
import de.julielab.elastic.query.components.data.ISearchServerDocument;
import de.julielab.elastic.query.components.data.SearchServerRequest;
import de.julielab.elastic.query.components.data.SortCommand;
import de.julielab.elastic.query.components.data.query.MatchAllQuery;
import de.julielab.elastic.query.services.ElasticSearchClientProvider;
import de.julielab.elastic.query.services.IElasticServerResponse;
import de.julielab.java.utilities.FileUtilities;
import org.apache.tapestry5.ioc.internal.LoggerSourceImpl;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeepPaginationTest {
    public static final String TEST_INDEX = "testindex";
    public static final String TEST_CLUSTER = "testcluster";
    public static final int NUM_DOCS = 100000;
    private final static Logger log = LoggerFactory.getLogger(DeepPaginationTest.class);
    // in case we need to disable X-shield: https://stackoverflow.com/a/51172136/1314955
    @ClassRule
    public static GenericContainer es = new GenericContainer(
            new ImageFromDockerfile("elasticquerycomponentstest", true)
                    .withFileFromClasspath("Dockerfile", "dockercontext/Dockerfile")
    )
            .withExposedPorts(9200)
            .withStartupTimeout(Duration.ofMinutes(2))
            .withEnv("cluster.name", TEST_CLUSTER);
    private static ElasticSearchComponent<ElasticSearchCarrier<IElasticServerResponse>> esSearchComponent;

    @BeforeClass
    public static void setup() throws Exception {
        setupES();
    }

    private static void setupES() throws IOException, InterruptedException {
        Slf4jLogConsumer toStringConsumer = new Slf4jLogConsumer(log);
        es.followOutput(toStringConsumer, OutputFrame.OutputType.STDOUT);
        ObjectMapper om = new ObjectMapper();
        {
            // Create the test index
            URL url = new URL("http://localhost:" + es.getMappedPort(9200) + "/" + TEST_INDEX);

            String mapping = om.writeValueAsString(Map.of("mappings", Map.of("properties", Map.of("text", Map.of("type", "text", "store", true), "sortid", Map.of("type", "keyword", "store", "true")))));
            System.out.println(mapping);

            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod("PUT");
            urlConnection.setRequestProperty("Content-Type", "application/json");
            urlConnection.setDoOutput(true);
            IOUtils.write(mapping, urlConnection.getOutputStream(), UTF_8);
            log.info("Response for index creation: {}", urlConnection.getResponseMessage());

            if (urlConnection.getErrorStream() != null) {
                String error = IOUtils.toString(urlConnection.getErrorStream(), UTF_8);
                log.error("Error when creating index: {}", error);
            }

        }


        {
            // Index the test documents
            log.debug("Creating {} test documents for indexing", NUM_DOCS);

            int batchsize = 1000;
            int numIndexed = 0;
            List<String> bulkCommandLines = new ArrayList<>(batchsize);
            final List<Integer> ids = IntStream.range(0, NUM_DOCS).mapToObj(Integer::valueOf).collect(Collectors.toList());
            Collections.shuffle(ids);
            for (int i = 0; i < NUM_DOCS; i++) {
                String jsonContents = om.writeValueAsString(Map.of("text", "This is an example text for document " + i, "docnum", i, "sortid", "PMC" + ids.get(i) + "_1.0_0.1"));
                jsonContents = jsonContents.replaceAll("\n", "");
                Map<String, Object> indexMap = new HashMap<>();
                indexMap.put("_index", TEST_INDEX);
                indexMap.put("_id", i);
                Map<String, Object> map = new HashMap<>();
                map.put("index", indexMap);

                bulkCommandLines.add(om.writeValueAsString(map));
                bulkCommandLines.add(jsonContents);

                if (bulkCommandLines.size() % batchsize == 0 || i == NUM_DOCS - 1) {
                    URL url = new URL("http://localhost:" + es.getMappedPort(9200) + "/_bulk");
                    HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
                    urlConnection.setRequestProperty("Content-Type", "application/json");
                    urlConnection.setDoOutput(true);
                    urlConnection.setRequestMethod("POST");
                    OutputStream outputStream = urlConnection.getOutputStream();
                    IOUtils.writeLines(bulkCommandLines, System.getProperty("line.separator"), outputStream, "UTF-8");
                    log.trace("Response for indexing: {}", urlConnection.getResponseMessage());

                    numIndexed += bulkCommandLines.size()/2;
                    bulkCommandLines.clear();

                    if (numIndexed % (NUM_DOCS / 10) == 0)
                        log.debug("Indexed test {} documents", numIndexed);
                }
            }
        }
        // Wait for ES to finish its indexing
        Thread.sleep(2000);
        {
            URL url = new URL("http://localhost:" + es.getMappedPort(9200) + "/" + TEST_INDEX + "/_count");
            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            String countResponse = IOUtils.toString(urlConnection.getInputStream(), StandardCharsets.UTF_8);
            log.debug("Response for the count of documents: {}", countResponse);
            assertTrue(countResponse.contains("count\":" + NUM_DOCS));
        }
        esSearchComponent = new ElasticSearchComponent<>(LoggerFactory.getLogger(ElasticSearchComponent.class), new ElasticSearchClientProvider(LoggerFactory.getLogger(ElasticSearchClientProvider.class), new LoggerSourceImpl(), TEST_CLUSTER, "localhost", String.valueOf(es.getMappedPort(9200))));
    }

    @Test
    public void scroll() {

        final SearchServerRequest request = new SearchServerRequest();
        request.query = new MatchAllQuery();
        request.index = TEST_INDEX;
        request.downloadCompleteResults = true;
        request.downloadCompleteResultsMethod = "scroll";
        request.fieldsToReturn = List.of("text");
        request.trackTotalHitsUpTo = Integer.MAX_VALUE;
        request.rows = 500;
        request.sortCmds = List.of(new SortCommand("_doc", SortCommand.SortOrder.ASCENDING));

        final ElasticSearchCarrier<IElasticServerResponse> carrier = new ElasticSearchCarrier<>("testchain");
        carrier.addServerRequest(request);

        esSearchComponent.process(carrier);

        final IElasticServerResponse response = carrier.getSingleSearchServerResponse();
        final Iterator<ISearchServerDocument> iterator = response.getDocumentResults().iterator();
        int received = 0;
        long nanos = System.nanoTime();
        while (iterator.hasNext()) {
            ISearchServerDocument doc = iterator.next();
            ++received;
        }
        nanos = System.nanoTime() - nanos;
        log.info("Received {} documents in {}s", received, nanos / Math.pow(10, 9));
        assertEquals(NUM_DOCS, received);
    }

    @Test
    public void searchAfter() {

        final SearchServerRequest request = new SearchServerRequest();
        request.query = new MatchAllQuery();
        request.index = TEST_INDEX;
        request.downloadCompleteResults = true;
        request.downloadCompleteResultsMethod = "searchAfter";
        request.fieldsToReturn = List.of("text");
        request.trackTotalHitsUpTo = Integer.MAX_VALUE;
        request.sortCmds = List.of(new SortCommand("_shard_doc", SortCommand.SortOrder.ASCENDING));
        request.rows = 500;


        final ElasticSearchCarrier<IElasticServerResponse> carrier = new ElasticSearchCarrier<>("testchain");
        carrier.addServerRequest(request);

        esSearchComponent.process(carrier);

        final IElasticServerResponse response = carrier.getSingleSearchServerResponse();
        log.info("Total hits: {} with relation {}", response.getNumFound(), response.getNumFoundRelation());
        final Iterator<ISearchServerDocument> iterator = response.getDocumentResults().iterator();
        int received = 0;
        long nanos = System.nanoTime();
        while (iterator.hasNext()) {
            ISearchServerDocument doc = iterator.next();
            ++received;
        }
        nanos = System.nanoTime() - nanos;
        log.info("Received {} documents in {}s", received, nanos / Math.pow(10, 9));
        assertEquals(NUM_DOCS, received);
    }

    @Test
    public void searchAfterCustomSort() {

        final SearchServerRequest request = new SearchServerRequest();
        request.query = new MatchAllQuery();
        request.index = TEST_INDEX;
        request.downloadCompleteResults = true;
        request.downloadCompleteResultsMethod = "searchAfter";
        request.fieldsToReturn = List.of("text");
        request.trackTotalHitsUpTo = Integer.MAX_VALUE;
        request.sortCmds = List.of(new SortCommand("sortid", SortCommand.SortOrder.ASCENDING));
        request.rows = 500;


        final ElasticSearchCarrier<IElasticServerResponse> carrier = new ElasticSearchCarrier<>("testchain");
        carrier.addServerRequest(request);

        esSearchComponent.process(carrier);

        final IElasticServerResponse response = carrier.getSingleSearchServerResponse();
        log.info("Total hits: {} with relation {}", response.getNumFound(), response.getNumFoundRelation());
        final Iterator<ISearchServerDocument> iterator = response.getDocumentResults().iterator();
        int received = 0;
        long nanos = System.nanoTime();
        while (iterator.hasNext()) {
            ISearchServerDocument doc = iterator.next();
            ++received;
        }
        nanos = System.nanoTime() - nanos;
        log.info("Received {} documents in {}s", received, nanos / Math.pow(10, 9));
        assertEquals(NUM_DOCS, received);
    }

}
