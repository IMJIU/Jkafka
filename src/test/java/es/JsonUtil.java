package es;

import java.io.IOException;

import es.entity.Blog;
import es.entity.Video;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class JsonUtil {

    // Java实体对象转json对象
    public static String model2Json(Blog blog) {
        String jsonData = null;
        try {
            XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
            jsonBuild.startObject().field("id", blog.getId()).field("title", blog.getTitle())
                    .field("posttime", blog.getPosttime()).field("content",blog.getContent()).endObject();

            jsonData = jsonBuild.string();
            //System.out.println(jsonData);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return jsonData;
    }
    // Java实体对象转json对象
    public static String model2Json(Video video) {
        String jsonData = null;
        try {
            XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
            jsonBuild.startObject().field("id", video.getId())
                    .field("name", video.getName())
                    .field("nameInitial", video.getNameInitial())
                    .endObject();

            jsonData = jsonBuild.string();
            //System.out.println(jsonData);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return jsonData;
    }
}