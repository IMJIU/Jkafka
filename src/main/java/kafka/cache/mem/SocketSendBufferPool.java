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
package kafka.cache.mem;

import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.ref.SoftReference;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class SocketSendBufferPool {

    private static final SendBuffer EMPTY_BUFFER = new EmptySendBuffer();
    private static final SendBuffer NEED_BUFFER = new EmptySendBuffer();

    private static final int DEFAULT_PREALLOCATION_SIZE = 1024 * 1024;//1MB
    private static final int ALIGN_SHIFT = 4;
    private static final int ALIGN_MASK = 15;

    private PreAllocationRef poolHead;
    private PreAllocation current = new PreAllocation(DEFAULT_PREALLOCATION_SIZE);
    private AtomicInteger totalCacheSize = new AtomicInteger(DEFAULT_PREALLOCATION_SIZE);
    private final int MAX_CACHE_SIZE = 1024 * 1024 * 4;//共30MB

    public void info() {
        System.out.println("total:" + totalCacheSize + " Max:" + MAX_CACHE_SIZE);
    }

    public static void main(String[] args) throws InterruptedException {
        SocketSendBufferPool pool = new SocketSendBufferPool();
        Random r = new Random();
        while (true) {
            int len = 1024 * 100 + r.nextInt(10);
            System.out.print(len + "/t");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < len; i++) {
                sb.append(i);
            }
            pool.acquire(sb.toString().getBytes());
            pool.info();
            Thread.sleep(1000L);
        }
    }

    public SendBuffer acquire(byte[] src) {
        final int size = src.length;
        System.out.println(size);
        if (size == 0) {
            return EMPTY_BUFFER;
        }

        // TODO: 2017/10/31 大于块大小，用磁盘发送
        if (size > DEFAULT_PREALLOCATION_SIZE) {
            throw new LackMemException("lack mem");
        }

        PreAllocation current = this.current;
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
            this.current = current = getPreAllocation();
            buffer = current.buffer;
            ByteBuffer slice = buffer.duplicate();
            buffer.position(align(size));
            slice.limit(size);
            current.refCnt++;
            dst = new PooledSendBuffer(current, slice);
        } else { // size == remaining
            current.refCnt++;
            this.current = getPreAllocation0();
            dst = new PooledSendBuffer(current, current.buffer);
        }
        //放入数据
        ByteBuffer dstbuf = dst.buffer;
        dstbuf.mark();
        dstbuf.put(src);
        dstbuf.reset();
        return dst;
    }

    private PreAllocation getPreAllocation() {
        PreAllocation current = this.current;
        if (current.refCnt == 0) {
            current.buffer.clear();
            return current;
        }
        return getPreAllocation0();
    }

    private PreAllocation getPreAllocation0() {
        PreAllocationRef ref = poolHead;
        if (ref != null) {
            do {
                PreAllocation p = ref.get();
                ref = ref.next;
                if (p != null) {
                    poolHead = ref;
                    return p;
                }
            } while (ref != null);
            poolHead = ref;
        }
        System.out.println("total:" + totalCacheSize + " max:" + DEFAULT_PREALLOCATION_SIZE);
        if (totalCacheSize.get() == DEFAULT_PREALLOCATION_SIZE) {
            throw new LackMemException("lack mem");
        }
        totalCacheSize.addAndGet(DEFAULT_PREALLOCATION_SIZE);
        return new PreAllocation(DEFAULT_PREALLOCATION_SIZE);
    }

    private static int align(int pos) {
        int q = pos >>> ALIGN_SHIFT;
        int r = pos & ALIGN_MASK;
        if (r != 0) {
            q++;
        }
        return q << ALIGN_SHIFT;
    }

    private static final class PreAllocation {
        final ByteBuffer buffer;
        int refCnt;

        PreAllocation(int capacity) {
            buffer = ByteBuffer.allocateDirect(capacity);
        }
    }

    private final class PreAllocationRef extends SoftReference<PreAllocation> {
        final PreAllocationRef next;

        PreAllocationRef(PreAllocation preAllocation, PreAllocationRef next) {
            super(preAllocation);
            this.next = next;
        }
    }


    final class PooledSendBuffer extends UnpooledSendBuffer {

        private final PreAllocation parent;

        PooledSendBuffer(PreAllocation parent, ByteBuffer buffer) {
            super(buffer);
            this.parent = parent;
        }

        @Override
        public void release() {
            final PreAllocation parent = this.parent;
            if (--parent.refCnt == 0) {
                parent.buffer.clear();
                if (totalCacheSize.intValue() > MAX_CACHE_SIZE) {
                    totalCacheSize.addAndGet(-DEFAULT_PREALLOCATION_SIZE);
                } else {
                    if (parent != current) {
                        poolHead = new PreAllocationRef(parent, poolHead);
                    }
                }
            }
        }
    }


    static class UnpooledSendBuffer implements SendBuffer {

        final ByteBuffer buffer;
        final int initialPos;

        UnpooledSendBuffer(ByteBuffer buffer) {
            this.buffer = buffer;
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

        public final int transferTo(GatheringByteChannel ch) throws IOException {
            return ch.write(buffer);
        }

        public final long transferTo(WritableByteChannel ch) throws IOException {
            return ch.write(buffer);
        }

        public int transferTo(OutputStream outputStream) throws IOException {
            byte[] b = buffer.array();
            outputStream.write(b);
            return b.length;
        }

        public final long transferTo(DatagramChannel ch, SocketAddress raddr) throws IOException {
            return ch.send(buffer, raddr);
        }

        public void release() {
            // Unpooled.
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
            if (current.buffer.isDirect()) {
                ((DirectBuffer) current.buffer).cleaner().clean();
            } else {
                current.buffer.clear();
            }
        }
    }

}
