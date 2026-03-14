package producer.model;

import java.util.List;

public class TransactionBatch {

    private String loteId;
    private String fechaGeneracion;
    private List<Transaction> transacciones;

    public String getLoteId() {
        return loteId;
    }

    public void setLoteId(String loteId) {
        this.loteId = loteId;
    }

    public String getFechaGeneracion() {
        return fechaGeneracion;
    }

    public void setFechaGeneracion(String fechaGeneracion) {
        this.fechaGeneracion = fechaGeneracion;
    }

    public List<Transaction> getTransacciones() {
        return transacciones;
    }

    public void setTransacciones(List<Transaction> transacciones) {
        this.transacciones = transacciones;
    }
}