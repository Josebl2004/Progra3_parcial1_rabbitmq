package producer.model;

public class TransactionDetail {

    private String nombreBeneficiario;
    private String tipoTransferencia;
    private String descripcion;
    private TransactionReferences referencias;

    public String getNombreBeneficiario() {
        return nombreBeneficiario;
    }

    public void setNombreBeneficiario(String nombreBeneficiario) {
        this.nombreBeneficiario = nombreBeneficiario;
    }

    public String getTipoTransferencia() {
        return tipoTransferencia;
    }

    public void setTipoTransferencia(String tipoTransferencia) {
        this.tipoTransferencia = tipoTransferencia;
    }

    public String getDescripcion() {
        return descripcion;
    }

    public void setDescripcion(String descripcion) {
        this.descripcion = descripcion;
    }

    public TransactionReferences getReferencias() {
        return referencias;
    }

    public void setReferencias(TransactionReferences referencias) {
        this.referencias = referencias;
    }
}