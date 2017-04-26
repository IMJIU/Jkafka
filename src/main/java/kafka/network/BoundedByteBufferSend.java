package kafka.network;

import kafka.annotation.nonthreadsafe;

import java.nio.ByteBuffer;

/**
 * Created by zhoulf on 2017/4/26.
 */

@nonthreadsafe
public class BoundedByteBufferSend extends Send {
    public ByteBuffer buffer;


    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

    public BoundedByteBufferSend(ByteBuffer buffer) {
        this.buffer = buffer;
    }
    // Avoid possibility of overflow for 2GB-4 byte buffer;
        if(buffer.remaining >Integer.MAX_VALUE -sizeBuffer.limit)
            throw new

    IllegalStateException("Attempt to create a bounded buffer of "+buffer.remaining +" bytes, but the maximum "+
            "allowable size for a bounded buffer is "+(Integer.MAX_VALUE-sizeBuffer.limit) +".")
            sizeBuffer.putInt(buffer.limit);
        sizeBuffer.rewind();

    var Boolean
    complete =false;

    public void this(
    Int size)=this(ByteBuffer.allocate(size));

    public void this(
    RequestOrResponse request)=

    {
        this(request.sizeInBytes + ( if (request.requestId != None) 2
    else 0))
        request.requestId match {
        case Some(requestId) =>
            buffer.putShort(requestId);
        case None =>
    }

        request.writeTo(buffer);
        buffer.rewind();
    }


    public Integer

    void writeTo(GatheringByteChannel channel) {
        expectIncomplete();
        var written = channel.write(Array(sizeBuffer, buffer));
        // if we are done, mark it off;
        if (!buffer.hasRemaining)
            complete = true;
        written.asInstanceOf<Int>
    }

}
}
