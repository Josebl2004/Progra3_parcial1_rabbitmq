package consumer.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import consumer.model.TransactionPostRequest;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class TransactionPostClient {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String postUrl;

    public TransactionPostClient(HttpClient httpClient, ObjectMapper objectMapper, String postUrl) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.postUrl = postUrl;
        
    }
    

    public boolean sendTransaction(TransactionPostRequest requestBody) {
        try {
            String json = objectMapper.writeValueAsString(requestBody);

            System.out.println("----- JSON ENVIADO AL POST -----");
            System.out.println(json);
            System.out.println("--------------------------------");
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(postUrl))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(20))
                    .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            int status = response.statusCode();

            if (status >= 200 && status < 300) {
                System.out.println("POST exitoso. Status: " + status + " Respuesta: " + response.body());
                return true;
            }

            System.err.println("POST fallido. Status: " + status + " Body: " + response.body());
            return false;

        } catch (IOException | InterruptedException e) {
            System.err.println("Error enviando POST: " + e.getMessage());
            return false;
        }
    }
}
