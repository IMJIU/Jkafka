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

import kafka.common.KafkaException;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.SoftReference;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

final class SocketSendBufferPool {

    private static final SendBuffer EMPTY_BUFFER = new EmptySendBuffer();

    private static final int DEFAULT_PREALLOCATION_SIZE = 1024 * 1024;
    private static final int ALIGN_SHIFT = 4;
    private static final int ALIGN_MASK = 15;

    private PreAllocationRef poolHead;
    private PreAllocation current = new PreAllocation(DEFAULT_PREALLOCATION_SIZE);
    private AtomicInteger totalCacheSize = new AtomicInteger();
    private final int MAX_CACHE_SIZE = 1024 * 1024 * 30;//共30MB

    public static void main(String[] args) throws InterruptedException {
        testUnit();
    }

    private static void testUnit() throws InterruptedException {
        SocketSendBufferPool pool = new SocketSendBufferPool();
        int i = 0;
        Vector<SendBuffer> list = new Vector<>();
        new Thread(() -> {
            Random random = new Random();
            while (true) {
                try {
                    Thread.sleep(random.nextInt(4000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                while (list.size() > 0) {
                    SendBuffer sb = list.remove(0);
                    if (sb != null) sb.release();
                }
            }
        }).start();
        while (true) {
            byte[] ch = new byte[DEFAULT_PREALLOCATION_SIZE / 15];  //  100/15~=6 MB一秒
            SendBuffer sb = pool.acquire(ch);
            list.add(sb);
            if ((i++) % 100 == 0) {
                pool.info();
                Thread.sleep(1000);
            }
        }
    }

    private void info() {
        System.out.println("total:" + totalCacheSize + " Max:" + MAX_CACHE_SIZE);
    }


    private SendBuffer acquire(byte[] src) {
        final int size = src.length;
        if (size == 0) {
            return EMPTY_BUFFER;
        }

        if (size > DEFAULT_PREALLOCATION_SIZE) {
            return new UnpooledSendBuffer(src);
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
//    static class FileSendBuffer implements SendBuffer {
//        public volatile File file;
//        public FileChannel channel;
//        public Integer start;
//        public Integer end;
//        public Boolean isSlice;
//        /* the size of the message set in bytes */
//        private AtomicInteger _size;
//
//        FileSendBuffer(byte[] buffer) throws FileNotFoundException {
////            file = new File("d:/temp");
////            channel = new RandomAccessFile(file,"rw").getChannel();
////            // Ignore offset and size from input. We just want to write the whole buffer to the channel.
////            buffer.mark();
////            int written = 0;
////            try {
////                while (written < sizeInBytes()) {
////                    written += channel.write(buffer);
////                }
////                buffer.reset();
////            } catch (IOException e) {
////                logger.error(e.getMessage(),e);
////            }
////            return written;
//        }
//
//        public final boolean finished() {
//            return !buffer.hasRemaining();
//        }
//
//        public final long writtenBytes() {
//            return buffer.position() - initialPos;
//        }
//
//        public final long totalBytes() {
//            return buffer.limit() - initialPos;
//        }
//
//        public final long transferTo(WritableByteChannel ch) throws IOException {
//            // Ignore offset and size from input. We just want to write the whole buffer to the channel.
//            buffer.mark();
//            int written = 0;
//            try {
//                while (written < sizeInBytes()) {
//                    written += channel.write(buffer);
//                }
//                buffer.reset();
//            } catch (IOException e) {
//                logger.error(e.getMessage(),e);
//            }
//            return written;
//        }
//
//        public final long transferTo(DatagramChannel ch, SocketAddress raddr) throws IOException {
//            // Ignore offset and size from input. We just want to write the whole buffer to the channel.
//            buffer.mark();
//            int written = 0;
//            try {
//                while (written < sizeInBytes()) {
//                    written += channel.write(buffer);
//                }
//                buffer.reset();
//            } catch (IOException e) {
//                logger.error(e.getMessage(),e);
//            }
//            return written;
//        }
//
//        public void release() {
//            // Unpooled.
//        }
//
//    }
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

    interface SendBuffer {
        boolean finished();

        long writtenBytes();

        long totalBytes();

        long transferTo(WritableByteChannel ch) throws IOException;

        long transferTo(DatagramChannel ch, SocketAddress raddr) throws IOException;

        void release();
    }
}
