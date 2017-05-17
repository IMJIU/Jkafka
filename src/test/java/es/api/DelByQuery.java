package es.api;/**
 * Created by zhoulf on 2017/5/17.
 */


import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author
 * @create 2017-05-17 15:05
 **/
public class DelByQuery {
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


        String deletebyquery = "{\"query\": {\"match_all\": {}}}";
//        DeleteByQueryResponse response =  new DeleteByQueryRequestBuilder(client,
//                DeleteByQueryAction.INSTANCE)
//                .setIndices("blog")
//                .setTypes("article")
//                .setSource(deletebyquery)
//                .execute()
//                .actionGet();
    }

    /**
     * 删除field  清空
     */
    @Test
    public void del_field(){
        client.prepareUpdate("blog", "article", "10")
                .setScript(new Script(     "ctx._source.remove(\"content\")", ScriptService.ScriptType.INLINE, null, null))
                .get();
    }
    /**
     * 删除field  清空
     */
    @Test
    public void del_field2(){
        client.prepareUpdate("blog", "article", "10")
                .setScript(new Script(     "ctx._source.processInstance.remove(\"id\")",ScriptService.ScriptType.INLINE, null, null))
                .get();
    }

}
