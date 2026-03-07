package producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final String API_URL = "https://hly784ig9d.execute-api.us-east-1.amazonaws.com/default/transacciones";
    private static final Set<String> VALID_BANKS = Set.of("BANRURAL", "GYT", "BAC", "BI");
    private static final Set<String> processedIds = ConcurrentHashMap.newKeySet();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String rabbitHost = System.getenv().getOrDefault("RABBIT_HOST", "localhost");
        int rabbitPort = Integer.parseInt(System.getenv().getOrDefault("RABBIT_PORT", "5672"));
        String rabbitUser = System.getenv().getOrDefault("RABBIT_USER", "guest");
        String rabbitPassword = System.getenv().getOrDefault("RABBIT_PASSWORD", "guest");
        int pollingSeconds = Integer.parseInt(System.getenv().getOrDefault("POLLING_SECONDS", "10"));

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitHost);
        factory.setPort(rabbitPort);
        factory.setUsername(rabbitUser);
        factory.setPassword(rabbitPassword);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            for (String bank : VALID_BANKS) {
                channel.queueDeclare(bank, true, false, false, null);
            }

            System.out.println("Producer iniciado. RabbitMQ en " + rabbitHost + ":" + rabbitPort);

            while (true) {
                int published = fetchAndPublish(client, channel);
                System.out.println("Ciclo completado. Publicadas: " + published);
                TimeUnit.SECONDS.sleep(pollingSeconds);
            }
        }
    }

    private static int fetchAndPublish(HttpClient client, Channel channel) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(API_URL))
                    .GET()
                    .timeout(Duration.ofSeconds(20))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                System.err.println("API devolvio status " + response.statusCode());
                return 0;
            }

            JsonNode root = mapper.readTree(response.body());
            JsonNode transacciones = root.path("transacciones");
            if (!transacciones.isArray()) {
                System.err.println("El campo transacciones no es un arreglo.");
                return 0;
            }

            int published = 0;
            for (JsonNode tx : transacciones) {
                String id = tx.path("idTransaccion").asText("");
                String bank = tx.path("bancoDestino").asText("").toUpperCase().trim();
                if (id.isEmpty() || bank.isEmpty()) {
                    continue;
                }
                if (!VALID_BANKS.contains(bank)) {
                    System.out.println("Banco no soportado, se omite: " + bank + " (id: " + id + ")");
                    continue;
                }
                if (!processedIds.add(id)) {
                    continue;
                }

                byte[] payload = mapper.writeValueAsString(tx).getBytes(StandardCharsets.UTF_8);
                channel.basicPublish("", bank, null, payload);
                published++;
            }
            return published;
        } catch (Exception ex) {
            System.err.println("Error en fetchAndPublish: " + ex.getMessage());
            return 0;
        }
    }
}
