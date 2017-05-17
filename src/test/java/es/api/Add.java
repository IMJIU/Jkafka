package es.api;

import es.JsonUtil;
import es.entity.Blog;
import es.entity.Video;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;

public class Add {
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
    public void mapping() {
//        ImmutableOpenMap<String, MappingMetaData> mappings = client.admin().cluster().prepareState().execute()
//                .actionGet().getState().getMetaData().getIndices().get(indexname).getMappings();
//        mapping = mappings.get(typename).source().toString();
//        client.
    }

    /**
     * 创建索引名称
     *
     * @param indices 索引名称
     */
    public void createCluterName(String indices) {
        client.admin().indices().prepareCreate(indices).execute().actionGet();
        client.close();
    }

    /**
     * 创建mapping(feid("indexAnalyzer","ik")该字段分词IK索引 ；feid("searchAnalyzer","ik")该字段分词ik查询；具体分词插件请看IK分词插件说明)
     *
     * @param indices     索引名称；
     * @param mappingType 索引类型
     * @throws Exception
     */
    public void createMapping(String indices, String mappingType) throws Exception {
        new XContentFactory();
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(indices)
                .startObject("properties")
                .startObject("id").field("type", "integer").field("store", "yes").endObject()
                .startObject("kw").field("type", "string").field("store", "yes").endObject()
                .startObject("title").field("type", "string").field("store", "yes").endObject()
                .endObject()
                .endObject()
                .endObject();
        PutMappingRequest mapping = Requests.putMappingRequest(indices).type(mappingType).source(builder);
        client.admin().indices().putMapping(mapping).actionGet();
        client.close();
    }

    @Test
    public void create_mapping() throws Exception {
        createMapping("video", "info");
//        createCluterName("lianan");
    }

    @Test
    public void add() {
        List<String> jsonData = DataFactory.getInitJsonData();
        for (int i = 0; i < jsonData.size(); i++) {
            IndexResponse response = client.prepareIndex("blog", "article").setSource(jsonData.get(i)).get();
            if (response.isCreated()) {
                System.out.println("创建成功!");
            }
        }
        client.close();
    }

    @Test
    public void random_add() {
        List<String> jsonData = DataFactory.getInitJsonData();
        Random random = new Random();
        for (int i = 0; i < jsonData.size(); i++) {
            IndexResponse response = client.prepareIndex("blog", "article")
                    .setId(random.nextInt(1000) + "")
                    .setSource(jsonData.get(i)).get();
            if (response.isCreated()) {
                System.out.println("创建成功!");
            }
        }
        client.close();
    }

    @Test
    public void video_add() {
        String data1 = JsonUtil.model2Json(new Video(646781L, "ZLBLSHYXDSHJYRHT", "周立波律师回应吸毒：是喝酒有人黑他！"));
        IndexResponse response = client.prepareIndex("video", "info")
                .setId(646781 + "")
                .setSource(data1).get();
        if (response.isCreated()) {
            System.out.println("创建成功!");
        }
        client.close();
    }

}