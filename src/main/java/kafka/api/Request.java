package kafka.api;

public class Request {
    public static final Integer OrdinaryConsumerId = -1;
    public static final Integer DebuggingConsumerId = -2;

    // Broker ids are non-negative int.
    public static Boolean isValidBrokerId(Integer brokerId) {
        return brokerId >= 0;
    }
}