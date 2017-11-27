package es.api;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Before;
import org.junit.Test;

public class BulkIn {
    private Client client;

    @Before
    public void setup() {
        try {
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", "myapplication").build();// cluster.name在elasticsearch.yml中配置
            // client startup
             client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(
                            InetAddress.getByName("127.0.0.1"), 9300));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void bulk_file() {
        try {
            File article = new File("files/bulk.txt");
            FileReader fr = new FileReader(article);
            BufferedReader bfr = new BufferedReader(fr);
            String line = null;
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            int count = 0;
            while ((line = bfr.readLine()) != null) {
                bulkRequest.add(client.prepareIndex("test", "article").setSource(line));
                if (count % 10 == 0) {
                    bulkRequest.execute().actionGet();
                }
                count++;
                //System.out.println(line);
            }
            bulkRequest.execute().actionGet();
            bfr.close();
            fr.close();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void bulk_add() {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        List<String> jsonData = DataFactory.getInitJsonData();
        Random random = new Random();
        for (int i = 0; i < jsonData.size(); i++) {
            bulkRequest.add(client.prepareIndex("blog", "article", random.nextInt(1000) + "")
                    .setSource(jsonData.get(i)));
            if (i % 10 == 0) {
                bulkRequest.execute().actionGet();
            }
        }
        bulkRequest.execute().actionGet();
    }

}