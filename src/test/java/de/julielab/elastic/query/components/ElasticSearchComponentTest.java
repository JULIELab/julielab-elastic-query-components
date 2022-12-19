package de.julielab.elastic.query.components;

import de.julielab.elastic.query.components.data.ElasticSearchCarrier;
import de.julielab.elastic.query.components.data.ISearchServerDocument;
import de.julielab.elastic.query.components.data.SearchServerRequest;
import de.julielab.elastic.query.components.data.SortCommand;
import de.julielab.elastic.query.components.data.query.MatchAllQuery;
import de.julielab.elastic.query.components.data.query.TermQuery;
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticSearchComponentTest {
    public static final String TEST_INDEX = "testindex";
    public static final String TEST_CLUSTER = "testcluster";
    private final static Logger log = LoggerFactory.getLogger(ElasticSearchComponentTest.class);
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

        {
            // Create the test index
            URL url = new URL("http://localhost:" + es.getMappedPort(9200) + "/" + TEST_INDEX);

            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod("PUT");
            urlConnection.setRequestProperty("Content-Type", "application/json");
            urlConnection.setDoOutput(true);
            log.info("Response for index creation: {}", urlConnection.getResponseMessage());

            if (urlConnection.getErrorStream() != null) {
                String error = IOUtils.toString(urlConnection.getErrorStream(), UTF_8);
                log.error("Error when creating index: {}", error);
            }

        }


        {
            // Index the test documents
            File dir = new File("src/test/resources/test-documents");
            File[] testDocuments = dir.listFiles((dir1, name) -> name.endsWith("json"));
            log.debug("Reading {} test relation documents for indexing", testDocuments.length);
            List<String> bulkCommandLines = new ArrayList<>(testDocuments.length);
            ObjectMapper om = new ObjectMapper();
            for (File doc : testDocuments) {
                String jsonContents = IOUtils.toString(FileUtilities.getInputStreamFromFile(doc), UTF_8);
                jsonContents = jsonContents.replaceAll("\n", "");
                Map<String, Object> indexMap = new HashMap<>();
                indexMap.put("_index", TEST_INDEX);
                indexMap.put("_id", doc.getName().replace(".json", ""));
                Map<String, Object> map = new HashMap<>();
                map.put("index", indexMap);

                bulkCommandLines.add(om.writeValueAsString(map));
                bulkCommandLines.add(jsonContents);
            }
            log.debug("Indexing test documents");
            URL url = new URL("http://localhost:" + es.getMappedPort(9200) + "/_bulk");
            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestProperty("Content-Type", "application/json");
            urlConnection.setDoOutput(true);
            urlConnection.setRequestMethod("POST");
            OutputStream outputStream = urlConnection.getOutputStream();
            IOUtils.writeLines(bulkCommandLines, System.getProperty("line.separator"), outputStream, "UTF-8");
            log.debug("Response for indexing: {}", urlConnection.getResponseMessage());
        }
        // Wait for ES to finish its indexing
        Thread.sleep(2000);
        {
            URL url = new URL("http://localhost:" + es.getMappedPort(9200) + "/" + TEST_INDEX + "/_count");
            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            String countResponse = IOUtils.toString(urlConnection.getInputStream(), StandardCharsets.UTF_8);
            log.debug("Response for the count of documents: {}", countResponse);
            assertTrue(countResponse.contains("count\":3"));
        }
        esSearchComponent = new ElasticSearchComponent<>(LoggerFactory.getLogger(ElasticSearchComponent.class), new ElasticSearchClientProvider(LoggerFactory.getLogger(ElasticSearchClientProvider.class), new LoggerSourceImpl(), TEST_CLUSTER, "localhost", String.valueOf(es.getMappedPort(9200)), 30000));
    }

    @Test
    public void testCountApiIntegration() {

        final SearchServerRequest request = new SearchServerRequest();
        request.query = new MatchAllQuery();
        request.isCountRequest = true;
        request.index = TEST_INDEX;

        final ElasticSearchCarrier<IElasticServerResponse> carrier = new ElasticSearchCarrier<>("testchain");
        carrier.addServerRequest(request);

        esSearchComponent.process(carrier);

        assertEquals(1, carrier.getSearchResponses().size());

        final IElasticServerResponse response = carrier.getSingleSearchServerResponse();

        assertTrue(response.isCountResponse());

        assertEquals(3, response.getNumFound());
    }

    @Test
    public void testEmptySearchAfter() {

        final SearchServerRequest request = new SearchServerRequest();
        request.query = new TermQuery();
        ((TermQuery) request.query).field = "doesnotexist";
        ((TermQuery) request.query).term = "anything";
        request.downloadCompleteResults = true;
        request.downloadCompleteResultsMethod = "searchAfter";
        request.addSortCommand("_shard_doc", SortCommand.SortOrder.ASCENDING);
        request.index = TEST_INDEX;

        final ElasticSearchCarrier<IElasticServerResponse> carrier = new ElasticSearchCarrier<>("testchain");
        carrier.addServerRequest(request);

        esSearchComponent.process(carrier);

        assertEquals(1, carrier.getSearchResponses().size());

        final IElasticServerResponse response = carrier.getSingleSearchServerResponse();

        // there are no results
        assertEquals(0, response.getNumFound());
        // But we still try to iterate over the documents. This was cause of error in the past.
        final Iterator<ISearchServerDocument> docIt = response.getDocumentResults().iterator();
        while (docIt.hasNext()) {
            final ISearchServerDocument ignored = docIt.next();
        }
    }

    @Test
    public void testDeepPaginationLimit() {

        final SearchServerRequest request = new SearchServerRequest();
        request.query = new MatchAllQuery();
        request.downloadCompleteResults = true;
        request.downloadCompleteResultsMethod = "searchAfter";
        request.downloadCompleteResultsLimit = 2;
        request.addSortCommand("_shard_doc", SortCommand.SortOrder.ASCENDING);
        request.index = TEST_INDEX;

        final ElasticSearchCarrier<IElasticServerResponse> carrier = new ElasticSearchCarrier<>("testchain");
        carrier.addServerRequest(request);

        esSearchComponent.process(carrier);

        assertEquals(1, carrier.getSearchResponses().size());

        final IElasticServerResponse response = carrier.getSingleSearchServerResponse();

        // there should be more results than we got
        assertEquals(3, response.getNumFound());
        // But we still try to iterate over the documents. This was cause of error in the past.
        final Iterator<ISearchServerDocument> docIt = response.getDocumentResults().iterator();
        int received = 0;
        while (docIt.hasNext()) {
            final ISearchServerDocument ignored = docIt.next();
            ++received;
        }
        assertEquals(2, received);
    }

}
