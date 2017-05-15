package kafka.server;/**
 * Created by zhoulf on 2017/5/15.
 */

import kafka.utils.Logging;

/**
 * @author
 * @create 2017-05-15 15:54
 **/
public class StateChangeLogger extends Logging{
    String loggerName;

    public StateChangeLogger(String loggerName) {
        this.loggerName = loggerName;
    }
}
