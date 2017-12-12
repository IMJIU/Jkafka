package kafka.log;/**
 * Created by zhoulf on 2017/4/12.
 */

import com.google.common.collect.Maps;
import kafka.utils.Logging;

import java.io.*;
import java.util.Collections;
import java.util.Map;

/**
 * This class saves out a map of topic/partition=>offsets to a file
 */
public class OffsetCheckpoint extends Logging {
    public File file;
    private Object lock = new Object();

    public OffsetCheckpoint(File file) {
        this.file = file;
        new File(file + ".tmp").delete(); // try to delete any existing temp files for cleanliness;
        try {
            file.createNewFile(); // in case the file doesn't exist;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void write(Map<TopicAndPartition, Long> offsets) throws IOException {
        synchronized (lock) {
            // write to temp file and then swap with the existing file;
            File temp = new File(file.getAbsolutePath() + ".tmp");

            FileOutputStream fileOutputStream = new FileOutputStream(temp);
            final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
            try {
                // write the current version;
                writer.write("0");
                // write the number of entries;
                writer.write(offsets.size());
                writer.newLine();

                // write the entries;
                offsets.forEach((topicPart, offset) -> {
                    try {
                        writer.write(String.format("%s %d %d", topicPart.topic, topicPart.partition, offset));
                        writer.newLine();
                    } catch (IOException e) {
                        error(e.getMessage(),e);
                    }
                });

                // flush the buffer and then fsync the underlying file;
                writer.flush();
                fileOutputStream.getFD().sync();
            } finally {
                writer.close();
            }

            // swap new offset checkpoint file with previous one;
            if (!temp.renameTo(file)) {
                // renameTo() fails on Windows if the destination file exists.;
                file.delete();
                if (!temp.renameTo(file))
                    throw new IOException(String.format("File rename from %s to %s failed.", temp.getAbsolutePath(), file.getAbsolutePath()));
            }
        }
    }

    public Map<TopicAndPartition, Long> read() throws IOException {
        synchronized (lock) {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            try {
                String line = reader.readLine();
                if (line == null)
                    return Collections.EMPTY_MAP;
                Integer version = Integer.parseInt(line);
                if (version.equals(0)) {
                    line = reader.readLine();
                    if (line == null)
                        return Collections.EMPTY_MAP;
                    Integer expectedSize = Integer.parseInt(line);
                    Map<TopicAndPartition, Long> offsets = Maps.newHashMap();
                    line = reader.readLine();
                    while (line != null) {
                        String[] pieces = line.split("\\s+");
                        if (pieces.length != 3)
                            throw new IOException(String.format("Malformed line in offset checkpoint file: '%s'.", line));
                        String topic = pieces[0];
                        Integer partition = Integer.parseInt(pieces[1]);
                        Long offset = Long.parseLong(pieces[2]);
                        offsets.put(new TopicAndPartition(topic, partition), offset);
                        line = reader.readLine();
                    }
                    if (offsets.size() != expectedSize)
                        throw new IOException(String.format("Expected %d entries but found only %d", expectedSize, offsets.size()));
                    return offsets;
                } else {
                    throw new IOException("Unrecognized version of the highwatermark checkpoint file: " + version);
                }
            } finally {
                reader.close();
            }
        }
    }
}
