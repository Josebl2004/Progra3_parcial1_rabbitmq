package producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import producer.client.TransactionApiClient;
import producer.config.RabbitMQConfig;
import producer.service.TransactionPublisherService;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final String API_URL =
            "https://hly784ig9d.execute-api.us-east-1.amazonaws.com/default/transacciones";

    public static void main(String[] args) {
        String rabbitHost = System.getenv().getOrDefault("RABBIT_HOST", "localhost");
        int rabbitPort = Integer.parseInt(System.getenv().getOrDefault("RABBIT_PORT", "5673"));
        String rabbitUser = System.getenv().getOrDefault("RABBIT_USER", "admin");
        String rabbitPassword = System.getenv().getOrDefault("RABBIT_PASSWORD", "admin");
        int pollingSeconds = Integer.parseInt(System.getenv().getOrDefault("POLLING_SECONDS", "10"));

        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        ObjectMapper objectMapper = new ObjectMapper();

        RabbitMQConfig rabbitMQConfig = new RabbitMQConfig(rabbitHost, rabbitPort, rabbitUser, rabbitPassword);
        TransactionApiClient apiClient = new TransactionApiClient(httpClient, objectMapper, API_URL);
        TransactionPublisherService publisherService = new TransactionPublisherService(objectMapper);

        try (Connection connection = rabbitMQConfig.createConnection();
             Channel channel = connection.createChannel()) {

            System.out.println("Producer iniciado. RabbitMQ en " + rabbitHost + ":" + rabbitPort);

            while (true) {
                try {
                    int published = publisherService.fetchAndPublish(apiClient, channel);
                    System.out.println("Ciclo completado. Publicadas: " + published);
                } catch (Exception e) {
                    System.err.println("Error en ciclo principal del producer: " + e.getMessage());
                }

                TimeUnit.SECONDS.sleep(pollingSeconds);
            }

        } catch (Exception e) {
            System.err.println("No fue posible iniciar el producer: " + e.getMessage());
        }
    }
}
