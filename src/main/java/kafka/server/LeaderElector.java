package kafka.server;

/**
 * @author zhoulf
 * @create 2017-11-09 19:22
 **/
public interface LeaderElector {
    void startup();

    Boolean amILeader();

    Boolean elect();

    void close();
}
