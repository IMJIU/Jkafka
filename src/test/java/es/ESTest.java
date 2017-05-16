package es;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author
 * @create 2017-04-10 12:59
 **/
public class ESTest {
    public static TransportClient client() throws UnknownHostException {
        return TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host1"), 9200))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host2"), 9300));
    }
    public static void main(String[] args) throws  Exception{
        TransportClient client = client();
        try {
            GetResponse response = client.prepareGet("twitter", "tweet", "1").get();
            System.out.println(response);
        }finally {
            // on shutdown
            client.close();
        }





//        Settings settings = Settings.settingsBuilder()
//                .put("cluster.name", "myClusterName").build();
//        TransportClient client = TransportClient.builder().settings(settings).build();
////Add transport addresses and do something with the client...


//        Settings settings = Settings.settingsBuilder()
//                .put("client.transport.sniff", true).build();
//        TransportClient client = TransportClient.builder().settings(settings).build();



    }
}
