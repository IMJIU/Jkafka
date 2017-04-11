package kafka.utils;/**
 * Created by zhoulf on 2017/3/22.
 */

import com.google.common.collect.Maps;
import kafka.func.Action;
import kafka.func.Fun;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

/**
 * General helper functions!
 * <p>
 * This is for general helper functions that aren't specific to Kafka logic. Things that should have been included in
 * the standard library etc.
 * <p>
 * If you are making a new helper function and want to add it to this class please ensure the following:
 * 1. It has documentation
 * 2. It is the most general possible utility, not just the thing you needed in one particular place
 * 3. You have tests for it if it is nontrivial in any way
 */
public class Utils extends Logging {
    private static Logging logger = new Logging();

    /**
     * Wrap the given function in a java.lang.Runnable
     *
     * @param fun A function
     * @return A Runnable that just executes the function
     */
    public static Runnable runnable(Action fun) {
        return () -> fun.invoke();
    }
//
//        /**
//         * Create a daemon thread
//         * @param runnable The runnable to execute in the background
//         * @return The unstarted thread
//         */
//        public Thread  daemonThread(Runnable runnable)
//                newThread(runnable, true);
//
//        /**
//         * Create a daemon thread
//         * @param name The name of the thread
//         * @param runnable The runnable to execute in the background
//         * @return The unstarted thread
//         */
//        public Thread  daemonThread(String name, Runnable runnable)
//                newThread(name, runnable, true);
//
//        /**
//         * Create a daemon thread
//         * @param name The name of the thread
//         * @param fun The runction to execute in the thread
//         * @return The unstarted thread
//         */
//        public Thread  daemonThread(String name, fun: () => Unit)
//                daemonThread(name, runnable(fun));
//

    /**
     * Create a new thread
     *
     * @param name     The name of the thread
     * @param runnable The work for the thread to do
     * @param daemon   Should the thread block JVM shutdown?
     * @return The unstarted thread
     */
    public static Thread newThread(String name, Runnable runnable, Boolean daemon) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("Uncaught exception in thread '" + t.getName() + "':", e);
            }
        });
        return thread;
    }
//
//        /**
//         * Create a new thread
//         * @param runnable The work for the thread to do
//         * @param daemon Should the thread block JVM shutdown?
//         * @return The unstarted thread
//         */
//        public Thread  newThread(Runnable runnable, Boolean daemon) {
//                Integer thread = new Thread(runnable);
//                thread.setDaemon(daemon);
//                thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
//                    def uncaughtException(Thread t, Throwable e) {
//                        error("Uncaught exception in thread '" + t.getName + "':", e);
//                    }
//                });
//                thread;
//        }
//

    /**
     * Read the given byte buffer into a byte array
     */
    public static byte[] readBytes(ByteBuffer buffer) {
        return readBytes(buffer, 0, buffer.limit());
    }

    /**
     * Read a byte array from the given offset and size in the buffer
     */
    public static byte[] readBytes(ByteBuffer buffer, Integer offset, Integer size) {
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, size);
        } else {
            buffer.mark();
            buffer.get(dest);
            buffer.reset();
        }
        return dest;
    }
//
//        /**
//         * Read a properties file from the given path
//         * @param filename The path of the file to read
//         */
//        public Properties  loadProps(String filename) {
//                Integer props = new Properties();
//                var InputStream propStream = null;
//        try {
//            propStream = new FileInputStream(filename);
//            props.load(propStream);
//        } finally {
//            if(propStream != null)
//                propStream.close;
//        }
//        props;
//   }
//

    /**
     * Open a channel for the given file
     */
    public static FileChannel openChannel(File file, Boolean mutable) throws FileNotFoundException {
        if (mutable)
            return new RandomAccessFile(file, "rw").getChannel();
        else
            return new FileInputStream(file).getChannel();
    }

    //
//        /**
//         * Do the given action and log any exceptions thrown without rethrowing them
//         * @param log The log method to use for logging. E.g. logger.warn
//         * @param action The action to execute
//         */
    public static void swallow(Action action) {
        try {
            action.invoke();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
//
//        /**
//         * Test if two byte buffers are equal. In this case equality means having
//         * the same bytes from the current position to the limit
//         */
//        public Boolean  equal(ByteBuffer b1, ByteBuffer b2) {
//        // two byte buffers are equal if their position is the same,
//        // their remaining bytes are the same, and their contents are the same;
//        if(b1.position != b2.position)
//            return false;
//        if(b1.remaining != b2.remaining)
//            return false;
//        for(i <- 0 until b1.remaining);
//        if(b1.get(i) != b2.get(i))
//            return false;
//        return true;
//  }
//

    /**
     * Translate the given buffer into a string
     *
     * @param buffer   The buffer to translate
     * @param encoding The encoding to use in translating bytes to characters
     */
    public static String readString(ByteBuffer buffer, String encoding) {
        if (encoding == null) {
            encoding = Charset.defaultCharset().toString();
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        try {
            return new String(bytes, encoding);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String readString(ByteBuffer buffer) {
        return readString(buffer, null);
    }
//
//        /**
//         * Print an error message and shutdown the JVM
//         * @param message The error message
//         */
//        def croak(String message) {
//            System.err.println(message);
//            System.exit(1);
//        }
//

    /**
     * Recursively delete the given file/directory and any subfiles (if any exist)
     *
     * @param file The root file at which to begin deleting
     */
    public static void rm(String file) {
        rm(new File(file));
    }

    /**
     * Recursively delete the list of files/directories and any subfiles (if any exist)
     *
     * @param files sequence of files to be deleted
     */
    public static void rm(List<String> files) {
        files.stream().forEach(f -> rm(new File(f)));
    }

    /**
     * Recursively delete the given file/directory and any subfiles (if any exist)
     *
     * @param file The root file at which to begin deleting
     */
    public static void rm(File file) {
        if (file == null) {
            return;
        } else if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files)
                    rm(f);
            }
            file.delete();
        } else {
            file.delete();
        }
    }
//
//        /**
//         * Register the given mbean with the platform mbean server,
//         * unregistering any mbean that was there before. Note,
//         * this method will not throw an exception if the registration
//         * fails (since there is nothing you can do and it isn't fatal),
//         * instead it just returns false indicating the registration failed.
//         * @param mbean The object to register as an mbean
//         * @param name The name to register this mbean with
//         * @return true if the registration succeeded
//         */
//        public Boolean  registerMBean(Object mbean, String name) {
//        try {
//            Integer mbs = ManagementFactory.getPlatformMBeanServer();
//            mbs synchronized {
//                Integer objName = new ObjectName(name);
//                if(mbs.isRegistered(objName))
//                    mbs.unregisterMBean(objName);
//                mbs.registerMBean(mbean, objName);
//                true;
//            }
//        } catch {
//            case Exception e => {
//                error("Failed to register Mbean " + name, e);
//                false;
//            }
//        }
//  }
//
//        /**
//         * Unregister the mbean with the given name, if there is one registered
//         * @param name The mbean name to unregister
//         */
//        def unregisterMBean(String name) {
//            Integer mbs = ManagementFactory.getPlatformMBeanServer();
//            mbs synchronized {
//                Integer objName = new ObjectName(name);
//                if(mbs.isRegistered(objName))
//                    mbs.unregisterMBean(objName);
//            }
//        }

    /**
     * Read an unsigned integer from the current position in the buffer,
     * incrementing the position by 4 bytes
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static Long readUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }


    /**
     * Read an unsigned integer from the given position without modifying the buffers
     * position
     *
     * @param buffer the buffer to read from
     * @param index  the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static Long readUnsignedInt(ByteBuffer buffer, Integer index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param value  The value to write
     */
    public static void writetUnsignedInt(ByteBuffer buffer, Long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index  The position in the buffer at which to begin writing
     * @param value  The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, int index, Long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Compute the CRC32 of the byte array
     *
     * @param bytes The array to compute the checksum for
     * @return The CRC32
     */
    public static Long crc32(byte[] bytes) {
        return crc32(bytes, 0, bytes.length);
    }


    /**
     * Compute the CRC32 of the segment of the byte array given by the specificed size and offset
     *
     * @param bytes  The bytes to checksum
     * @param offset the offset at which to begin checksumming
     * @param size   the number of bytes to checksum
     * @return The CRC32
     */
    public static Long crc32(byte[] bytes, int offset, int size) {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }

    /**
     * Compute the hash code for the given items
     */
//    def hashcode(Any as*);
//
//    :Int=
//
//    {
//        if (as == null);
//            return 0;
//        var h = 1;
//        var i = 0;
//        while (i < as.length) {
//            if (as(i) != null) {
//                h = 31 * h + as(i).hashCode;
//                i += 1;
//            }
//        }
//        return h;
//    }
//
//    /**
//     * Group the given values by keys extracted with the given function
//     */
//    def groupby[
//    K,V](Iterable vals[V],V f=>K):Map[K,List[V]]=
//
//    {
//        Integer m = new mutable.HashMap[K, List[ V]];
//        for (v< -vals) {
//            Integer k = f(v);
//            m.get(k) match {
//                case Some(List l[V])=>m.put(k, v::l);
//                case None =>m.put(k, List(v));
//            }
//        }
//        m;
//    }
//
//    /**
//     * Read some bytes into the provided buffer, and return the number of bytes read. If the
//     * channel has been closed or we get -1 on the read for any reason, throw an EOFException
//     */
//    def read(ReadableByteChannel channel, ByteBuffer buffer);
//
//    :Int=
//
//    {
//        channel.read(buffer) match {
//        case -1 =>throw new EOFException("Received -1 when reading from channel, socket has likely been closed.");
//        case n:;
//            Integer =>n;
//    }
//    }
//
//    /**
//     * Throw an exception if the given value is null, else return it. You can use this like:
//     * Integer myValue = Utils.notNull(expressionThatShouldntBeNull)
//     */
//    def notNull[
//    V](V v)=
//
//    {
//        if (v == null);
//            throw new KafkaException("Value cannot be null.");
//        else;
//            v;
//    }
//
//    /**
//     * Get the stack trace from an exception as a string
//     */
//    def stackTrace(Throwable e);
//
//    :String=
//
//    {
//        Integer sw = new StringWriter;
//        Integer pw = new PrintWriter(sw);
//        e.printStackTrace(pw);
//        sw.toString();
//    }
//

    /**
     * This method gets comma separated values which contains key,value pairs and returns a map of
     * key value pairs. the format of allCSVal is val1 key1, val2 key2 ....
     */
    public static Map<String, String> parseCsvMap(String str) {
        HashMap map = Maps.newHashMap();
        if ("".equals(str))
            return map;
        List<String[]> keyVals = Arrays.asList(str.split("\\s*,\\s*")).stream().map(s -> s.split("\\s*:\\s*")).collect(Collectors.toList());
        for (String[] arr : keyVals) {
            map.put(arr[0], arr[1]);
        }
        return map;
    }
//

    /**
     * Parse a comma separated string into a sequence of strings.
     * Whitespace surrounding the comma will be removed.
     */
    public static List<String> parseCsvList(String csvList) {
        if (csvList == null || csvList.isEmpty())
            return Collections.EMPTY_LIST;
        else {
            return Arrays.asList(csvList.split("\\s*,\\s*")).stream().filter(v -> !v.equals("")).collect(Collectors.toList());
        }
    }
//
//    /**
//     * Create an instance of the class with the given class name
//     */
//    def createObject[
//    T<:AnyRef](String className,AnyRef args*):T=
//
//    {
//        Integer klass = Class.forName(className).asInstanceOf[Class[T]];
//        Integer constructor = klass.getConstructor(args.map(_.getClass):_ *);
//        constructor.newInstance(_ args *).asInstanceOf[T];
//    }
//
//    /**
//     * Is the given string null or empty ("")?
//     */
//    def nullOrEmpty(String s);
//
//    :Boolean=s==null||s.equals("");
//
//    /**
//     * Create a circular (looping) iterator over a collection.
//     *
//     * @param coll An iterable over the underlying collection.
//     * @return A circular iterator over the collection.
//     */
//    def circularIterator[
//    T](Iterable coll[T])=
//
//    {
//        Integer Stream stream[T] =
//        for (forever< -Stream.continually(1); t < -coll) yield t;
//        stream.iterator;
//    }
//
//    /**
//     * Attempt to read a file as a string
//     */
//    def readFileAsString(String path, Charset charset=Charset.defaultCharset();
//
//    ):String=
//
//    {
//        Integer stream = new FileInputStream(new File(path));
//        try {
//            Integer fc = stream.getChannel();
//            Integer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
//            charset.decode(bb).toString();
//        } finally {
//            stream.close();
//        }
//    }
//
//    /**
//     * Get the absolute value of the given number. If the number is Int.MinValue return 0.
//     * This is different from java.lang.Math.abs or scala.math.abs in that they return Int.MinValue (!).
//     */
//    def abs(Int n);
//
//    =if(n==Integer.MIN_VALUE)0 else math.abs(n)
//

    /**
     * Replace the given string suffix with the new suffix. If the string doesn't end with the given suffix throw an exception.
     */
    public static String replaceSuffix(String s, String oldSuffix, String newSuffix) {
        if (!s.endsWith(oldSuffix))
            throw new IllegalArgumentException(String.format("Expected string to end with '%s' but string is '%s'", oldSuffix, s));
        return s.substring(0, s.length() - oldSuffix.length()) + newSuffix;
    }

    //
//    /**
//     * Create a file with the given path
//     *
//     * @param path The path to create
//     * @return The created file
//     * @throws KafkaStorageException If the file create fails
//     */
//    def createFile(String path);
//
//    :File=
//
//    {
//        Integer f = new File(path);
//        Integer created = f.createNewFile();
//        if (!created);
//            throw new KafkaStorageException(String.format("Failed to create file %s.",path));
//        f;
//    }
//
//    /**
//     * Turn a properties map into a string
//     */
//    def asString(Properties props);
//
//    :String=
//
//    {
//        Integer writer = new StringWriter();
//        props.store(writer, "");
//        writer.toString;
//    }
//
//    /**
//     * Read some properties with the given default values
//     */
//    def readProps(String s, Properties defaults);
//
//    :Properties=
//
//    {
//        Integer reader = new StringReader(s);
//        Integer props = new Properties(defaults);
//        props.load(reader);
//        props;
//    }
//

    /**
     * Read a big-endian integer from a byte array
     */
    public static Integer readInt(byte[] bytes, Integer offset) {
        return ((bytes[offset] & 0xFF) << 24) |
                ((bytes[offset + 1] & 0xFF) << 16) |
                ((bytes[offset + 2] & 0xFF) << 8) |
                (bytes[offset + 3] & 0xFF);
    }

    //
//    /**
//     * Execute the given function inside the lock
//     */
    public static <T> T inLock(Lock lock, Fun<T> process) {
        lock.lock();
        try {
            return process.invoke();
        } finally {
            lock.unlock();
        }
    }

    public static void inLock(Lock lock, Action action) {
        lock.lock();
        try {
            action.invoke();
        } finally {
            lock.unlock();
        }
    }

//
//    def inReadLock[
//    T](ReadWriteLock lock)(fun:=>T):T=inLock[T](lock.readLock)(fun);
//
//    def inWriteLock[
//    T](ReadWriteLock lock)(fun:=>T):T=inLock[T](lock.writeLock)(fun);
//
//
//    //JSON strings need to be escaped based on ECMA-404 standard http://json.org;
//    def JSONEscapeString(String s);
//
//    :String=
//
//    {
//        s.map {
//        case '"' =>"\\\"";
//        case '\\' =>"\\\\";
//        case '/' =>"\\/";
//        case '\b' =>"\\b";
//        case '\f' =>"\\f";
//        case '\n' =>"\\n";
//        case '\r' =>"\\r";
//        case '\t' =>"\\t";
//      /* We'll unicode escape any control characters. These include:
//       * 0x0 -> 0x1f  : ASCII Control (C0 Control Codes)
//       * 0x7f         : ASCII DELETE
//       * 0x80 -> 0x9f : C1 Control Codes
//       *
//       * Per RFC4627, section 2.5, we're not technically required to
//       * encode the C1 codes, but we do to be safe.
//       */
//        case c if ((c >= '\u0000' && c <= '\u001f') || (c >= '\u007f' && c <= '\u009f'))=>String.format("\\u%04x",Int c);
//        case c =>c;
//    }.mkString;
//    }
//
//    /**
//     * Returns a list of duplicated items
//     */
//    def duplicates[
//    T](Traversable s[T]):Iterable[T]=
//
//    {
//        s.groupBy(identity);
//                .map {
//        case (k,l)=>(k, l.size)}
//        .filter {
//        case (k,l)=>(l > 1);
//    }
//        .keys;
//    }

    /*********************************************************************************/


    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index  The position in the buffer at which to begin writing
     * @param value  The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Write an unsigned integer in little-endian format to the {@link OutputStream}.
     *
     * @param out   The stream to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(OutputStream out, int value) throws IOException {
        out.write(value >>> 8 * 0);
        out.write(value >>> 8 * 1);
        out.write(value >>> 8 * 2);
        out.write(value >>> 8 * 3);
    }

    /**
     * Write an unsigned integer in little-endian format to a byte array
     * at a given offset.
     *
     * @param buffer The byte array to write to
     * @param offset The position in buffer to write to
     * @param value  The value to write
     */
    public static void writeUnsignedIntLE(byte[] buffer, int offset, int value) {
        buffer[offset++] = (byte) (value >>> 8 * 0);
        buffer[offset++] = (byte) (value >>> 8 * 1);
        buffer[offset++] = (byte) (value >>> 8 * 2);
        buffer[offset] = (byte) (value >>> 8 * 3);
    }

    /**
     * Read an unsigned integer from the given position without modifying the buffers position
     *
     * @param buffer the buffer to read from
     * @param index  the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    /**
     * Read an unsigned integer stored in little-endian format from the {@link InputStream}.
     *
     * @param in The stream to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(InputStream in) throws IOException {
        return (in.read() << 8 * 0)
                | (in.read() << 8 * 1)
                | (in.read() << 8 * 2)
                | (in.read() << 8 * 3);
    }

    /**
     * Read an unsigned integer stored in little-endian format from a byte array
     * at a given offset.
     *
     * @param buffer The byte array to read from
     * @param offset The position in buffer to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(byte[] buffer, int offset) {
        return (buffer[offset++] << 8 * 0)
                | (buffer[offset++] << 8 * 1)
                | (buffer[offset++] << 8 * 2)
                | (buffer[offset] << 8 * 3);
    }

}
