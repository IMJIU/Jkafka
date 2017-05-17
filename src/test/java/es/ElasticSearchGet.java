package es;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;

//import static org.elasticsearch.index.query.QueryBuilders.*;

public class ElasticSearchGet {


    private Client client;

    @Before
    public void setup() {
        try {
            // client startup
            client = TransportClient.builder().build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void get() {
        QueryBuilder qb;
        qb = QueryBuilders.termQuery("title", "hibernate");
//             qb = QueryBuilders.multiMatchQuery("git", "title", "content");

        SearchResponse response = client.prepareSearch("blog").setTypes("article")
                .setQuery(qb).execute().actionGet();
        SearchHits hits = response.getHits();
        if (hits.totalHits() > 0) {
            for (SearchHit hit : hits) {
                System.out.println("score:" + hit.getScore() + ":\t" + hit.getSource());// .get("title")
            }
        } else {
            System.out.println("搜到0条结果");
        }
    }

}