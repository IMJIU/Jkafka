package es.entity;/**
 * Created by zhoulf on 2017/5/17.
 */

/**
 * @author
 * @create 2017-05-17 16:58
 **/
public class Video {
    Long id;
    String nameInitial;
    String name;

    public Video(Long id, String nameInitial, String name) {
        this.id = id;
        this.nameInitial = nameInitial;
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNameInitial() {
        return nameInitial;
    }

    public void setNameInitial(String nameInitial) {
        this.nameInitial = nameInitial;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
