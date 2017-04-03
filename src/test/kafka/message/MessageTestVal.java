package kafka.message;

import kafka.message.CompressionCodec;
import kafka.message.Message;

public class MessageTestVal {
    public byte[] key;
    public byte[] payload;
    public CompressionCodec codec;
    public Message message;

    public MessageTestVal(byte[] key, byte[] payload, CompressionCodec codec, Message message) {
        this.key = key;
        this.payload = payload;
        this.codec = codec;
        this.message = message;
    }
}