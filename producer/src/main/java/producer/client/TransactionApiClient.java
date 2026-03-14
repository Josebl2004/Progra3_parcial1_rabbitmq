package producer.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import producer.model.TransactionBatch;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class TransactionApiClient {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String apiUrl;

    public TransactionApiClient(HttpClient httpClient, ObjectMapper objectMapper, String apiUrl) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.apiUrl = apiUrl;
    }

    public TransactionBatch fetchTransactions() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(apiUrl))
                .GET()
                .timeout(Duration.ofSeconds(20))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("La API devolvió status " + response.statusCode());
        }

        return objectMapper.readValue(response.body(), TransactionBatch.class);
    }
}