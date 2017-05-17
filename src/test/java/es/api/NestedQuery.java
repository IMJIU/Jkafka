package es.api;/**
 * Created by zhoulf on 2017/5/17.
 */

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.HasChildQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsLookupQueryBuilder;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author
 * @create 2017-05-17 15:05
 **/
public class NestedQuery {
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
    public void query() {
        TermsLookupQueryBuilder terms = QueryBuilders
                .termsLookupQuery("uuid")
                .lookupIndex("user")
                .lookupType("user")
                .lookupId("5")
                .lookupPath("uuids");

        HasChildQueryBuilder hQuery = QueryBuilders
                .hasChildQuery("instance", QueryBuilders
                        .hasChildQuery("instance_permission", terms));

        System.out.println("Exectuing Query 1");
        System.out.println(hQuery.toString());
        SearchResponse searchResponse1 = client
                .prepareSearch("foo_oa_hr_askforleave")
                .setQuery(hQuery).execute().actionGet();

        System.out.println("There were " + searchResponse1.getHits().getTotalHits()
                + " results found for Query 1.");
        System.out.println(searchResponse1.toString());
        System.out.println();
    }
}
