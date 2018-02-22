package kafka.common;

public class ControllerMovedException extends RuntimeException {
    public ControllerMovedException(String msg){
        super(msg);
    }
    public ControllerMovedException() {
        super();
    }

}
