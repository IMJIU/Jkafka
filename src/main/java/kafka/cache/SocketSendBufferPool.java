/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package kafka.cache;

import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;

final class SocketSendBufferPool {

    private static final SendBuffer EMPTY_BUFFER = new EmptySendBuffer();

    private static final int DEFAULT_PREALLOCATION_SIZE = 33;
    private static final int ALIGN_SHIFT = 4;
    private static final int ALIGN_MASK = 15;

    private PreallocationRef poolHead;
    private Preallocation current = new Preallocation(DEFAULT_PREALLOCATION_SIZE);

    public static void main(String[] args) {
        SocketSendBufferPool pool = new SocketSendBufferPool();
        byte[] ch = new String("11111111111111111111111").getBytes();
        SendBuffer sb = pool.acquire(ch);
        pool.acquire(ch);
        sb.release();
        pool.acquire(ch);
    }


    private SendBuffer acquire(byte[] src) {
        final int size = src.length;
        if (size == 0) {
            return EMPTY_BUFFER;
        }

        if (size > DEFAULT_PREALLOCATION_SIZE) {
            return new UnpooledSendBuffer(src);
        }

        Preallocation current = this.current;
        ByteBuffer buffer = current.buffer;
        int remaining = buffer.remaining();
        PooledSendBuffer dst;

        if (size < remaining) {
            int nextPos = buffer.position() + size;
            ByteBuffer slice = buffer.duplicate();
            buffer.position(align(nextPos));
            slice.limit(nextPos);
            current.refCnt++;
            dst = new PooledSendBuffer(current, slice);
        } else if (size > remaining) {
            this.current = current = getPreallocation();
            buffer = current.buffer;
            ByteBuffer slice = buffer.duplicate();
            buffer.position(align(size));
            slice.limit(size);
            current.refCnt++;
            dst = new PooledSendBuffer(current, slice);
        } else { // size == remaining
            current.refCnt++;
            this.current = getPreallocation0();
            dst = new PooledSendBuffer(current, current.buffer);
        }

        ByteBuffer dstbuf = dst.buffer;
        dstbuf.mark();
        dstbuf.put(src);
        dstbuf.reset();
        return dst;
    }

    private Preallocation getPreallocation() {
        Preallocation current = this.current;
        if (current.refCnt == 0) {
            current.buffer.clear();
            return current;
        }

        return getPreallocation0();
    }

    private Preallocation getPreallocation0() {
        PreallocationRef ref = poolHead;
        if (ref != null) {
            do {
                Preallocation p = ref.get();
                ref = ref.next;

                if (p != null) {
                    poolHead = ref;
                    return p;
                }
            } while (ref != null);

            poolHead = ref;
        }

        return new Preallocation(DEFAULT_PREALLOCATION_SIZE);
    }

    private static int align(int pos) {
        int q = pos >>> ALIGN_SHIFT;
        int r = pos & ALIGN_MASK;
        if (r != 0) {
            q++;
        }
        return q << ALIGN_SHIFT;
    }

    private static final class Preallocation {
        final ByteBuffer buffer;
        int refCnt;

        Preallocation(int capacity) {
            buffer = ByteBuffer.allocateDirect(capacity);
        }
    }

    private final class PreallocationRef extends SoftReference<Preallocation> {
        final PreallocationRef next;

        PreallocationRef(Preallocation prealloation, PreallocationRef next) {
            super(prealloation);
            this.next = next;
        }
    }

    interface SendBuffer {
        boolean finished();

        long writtenBytes();

        long totalBytes();

        long transferTo(WritableByteChannel ch) throws IOException;

        long transferTo(DatagramChannel ch, SocketAddress raddr) throws IOException;

        void release();
    }

    static class UnpooledSendBuffer implements SendBuffer {

        final ByteBuffer buffer;
        final int initialPos;

        UnpooledSendBuffer(ByteBuffer buffer) {
            this.buffer = buffer;
            this.buffer.put(buffer);
            initialPos = buffer.position();
        }

        UnpooledSendBuffer(byte[] buffer) {
            this.buffer = ByteBuffer.allocate(buffer.length);
            this.buffer.put(buffer);
            initialPos = 0;
        }

        public final boolean finished() {
            return !buffer.hasRemaining();
        }

        public final long writtenBytes() {
            return buffer.position() - initialPos;
        }

        public final long totalBytes() {
            return buffer.limit() - initialPos;
        }

        public final long transferTo(WritableByteChannel ch) throws IOException {
            return ch.write(buffer);
        }

        public final long transferTo(DatagramChannel ch, SocketAddress raddr) throws IOException {
            return ch.send(buffer, raddr);
        }

        public void release() {
            // Unpooled.
        }
    }

    final class PooledSendBuffer extends UnpooledSendBuffer {

        private final Preallocation parent;

        PooledSendBuffer(Preallocation parent, ByteBuffer buffer) {
            super(buffer);
            this.parent = parent;
        }

        @Override
        public void release() {
            final Preallocation parent = this.parent;
            if (--parent.refCnt == 0) {
                parent.buffer.clear();
                if (parent != current) {
                    poolHead = new PreallocationRef(parent, poolHead);
                }
            }
        }
    }

    static class GatheringSendBuffer implements SendBuffer {

        private final ByteBuffer[] buffers;
        private final int last;
        private long written;
        private final int total;

        GatheringSendBuffer(ByteBuffer[] buffers) {
            this.buffers = buffers;
            last = buffers.length - 1;
            int total = 0;
            for (ByteBuffer buf : buffers) {
                total += buf.remaining();
            }
            this.total = total;
        }

        public boolean finished() {
            return !buffers[last].hasRemaining();
        }

        public long writtenBytes() {
            return written;
        }

        public long totalBytes() {
            return total;
        }

        public long transferTo(WritableByteChannel ch) throws IOException {
            if (ch instanceof GatheringByteChannel) {
                long w = ((GatheringByteChannel) ch).write(buffers);
                written += w;
                return w;
            } else {
                int send = 0;
                for (ByteBuffer buf : buffers) {
                    if (buf.hasRemaining()) {
                        int w = ch.write(buf);
                        if (w == 0) {
                            break;
                        } else {
                            send += w;
                        }
                    }
                }
                written += send;
                return send;
            }
        }

        public long transferTo(DatagramChannel ch, SocketAddress raddr) throws IOException {
            int send = 0;
            for (ByteBuffer buf : buffers) {
                if (buf.hasRemaining()) {
                    int w = ch.send(buf, raddr);
                    if (w == 0) {
                        break;
                    } else {
                        send += w;
                    }
                }
            }
            written += send;

            return send;
        }

        public void release() {
            // nothing todo
        }
    }


    static final class EmptySendBuffer implements SendBuffer {

        public boolean finished() {
            return true;
        }

        public long writtenBytes() {
            return 0;
        }

        public long totalBytes() {
            return 0;
        }

        public long transferTo(WritableByteChannel ch) {
            return 0;
        }

        public long transferTo(DatagramChannel ch, SocketAddress raddr) {
            return 0;
        }

        public void release() {
            // Unpooled.
        }
    }

    public void releaseExternalResources() {
        if (current.buffer != null) {
            if(current.buffer.isDirect()){
                ((DirectBuffer)current.buffer).cleaner().clean();
            }else{
                current.buffer.clear();
            }
        }
    }
}
