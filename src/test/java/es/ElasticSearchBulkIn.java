package es;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Before;
import org.junit.Test;

public class ElasticSearchBulkIn {
    private Client client;

    @Before
    public void setup() {
        try {
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", "bropen").build();// cluster.name在elasticsearch.yml中配置
            // client startup
            Client client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(
                            InetAddress.getByName("127.0.0.1"), 9300));} catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Test
    public  void bulk() {
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
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}