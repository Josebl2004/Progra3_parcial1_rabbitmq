package producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import producer.client.TransactionApiClient;
import producer.model.Transaction;
import producer.model.TransactionBatch;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class TransactionPublisherService {

    private final ObjectMapper objectMapper;

    public TransactionPublisherService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public int fetchAndPublish(TransactionApiClient apiClient, Channel channel) throws Exception {
        TransactionBatch batch = apiClient.fetchTransactions();

        if (batch == null || batch.getTransacciones() == null || batch.getTransacciones().isEmpty()) {
            System.out.println("No se recibieron transacciones para publicar.");
            return 0;
        }

        List<Transaction> transactions = batch.getTransacciones();

        System.out.println("Lote recibido: " + batch.getLoteId()
                + " | Fecha: " + batch.getFechaGeneracion()
                + " | Total transacciones: " + transactions.size());

        int published = 0;

        for (Transaction transaction : transactions) {
            if (transaction == null) {
                continue;
            }

            String transactionId = safeTrim(transaction.getIdTransaccion());
            String bank = safeTrim(transaction.getBancoDestino()).toUpperCase();

            if (transactionId.isEmpty()) {
                System.err.println("Transacción omitida: idTransaccion vacío.");
                continue;
            }

            if (bank.isEmpty()) {
                System.err.println("Transacción omitida: bancoDestino vacío. ID: " + transactionId);
                continue;
            }

            channel.queueDeclare(bank, true, false, false, null);

            byte[] payload = objectMapper.writeValueAsString(transaction).getBytes(StandardCharsets.UTF_8);

            channel.basicPublish("", bank, MessageProperties.PERSISTENT_TEXT_PLAIN, payload);

            System.out.println("Transacción publicada. ID: " + transactionId + " -> Cola: " + bank);
            published++;
        }

        return published;
    }

    private String safeTrim(String value) {
        return value == null ? "" : value.trim();
    }
}