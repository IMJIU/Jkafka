package kafka.utils;/**
 * Created by zhoulf on 2017/3/23.
 */

import kafka.func.Action;
import kafka.func.ActionWithThrow;
import org.apache.log4j.Logger;

/**
 * @author
 * @create 2017-03-23 16:33
 **/
public class Logging {

    public Logging() {

    }
    public static Logging getLogger(String loggerName) {
        return new Logging(loggerName);
    }
    public Logging(String loggerName) {
        loggerName(loggerName);
    }

    private String loggerName;

    public void loggerName(String loggerName) {
        this.loggerName = loggerName;
        logger = Logger.getLogger(this.loggerName);
    }

    public String loggerName() {
        return this.getClass().getName();
    }

    public Logger logger = Logger.getLogger(loggerName());

    public String logIdent = null;

    // Force initialization to register Log4jControllerMBean
    private Log4jController log4jController;

    private String msgWithLogIdent(String msg) {
        if (logIdent == null)
            return msg;
        else
            return logIdent + msg;
    }

    public void trace(String msg) {
        if (logger.isTraceEnabled())
            logger.trace(msgWithLogIdent(msg));
    }

    public void trace(Throwable e) {
        if (logger.isTraceEnabled())
            logger.trace(logIdent, e);
    }

    public void trace(String msg, Throwable e) {
        if (logger.isTraceEnabled())
            logger.trace(msgWithLogIdent(msg), e);
    }

    public void debug(String msg) {
        if (logger.isDebugEnabled())
            logger.debug(msgWithLogIdent(msg));
    }

    public void debug(Throwable e) {
        if (logger.isDebugEnabled())
            logger.debug(logIdent, e);
    }

    public void debug(String msg, Throwable e) {
        if (logger.isDebugEnabled())
            logger.debug(msgWithLogIdent(msg), e);
    }

    public void swallowDebug(ActionWithThrow action) {
        Utils.swallow(action, (e) -> logger.debug(e.getMessage(), e));
    }


    public void info(String msg) {
        if (logger.isInfoEnabled())
            logger.info(msgWithLogIdent(msg));
    }

    public void info(Throwable e) {
        if (logger.isInfoEnabled())
            logger.info(logIdent, e);
    }

    public void info(String msg, Throwable e) {
        if (logger.isInfoEnabled())
            logger.info(msgWithLogIdent(msg), e);
    }

    public void swallowInfo(ActionWithThrow action) {
        Utils.swallow(action, (e) -> logger.info(e.getMessage(), e));
    }

    public void warn(String msg) {
        logger.warn(msgWithLogIdent(msg));
    }

    public void warn(Throwable e) {
        logger.warn(logIdent, e);
    }

    public void warn(String msg, Throwable e) {
        logger.warn(msgWithLogIdent(msg), e);
    }

    public void swallowWarn(ActionWithThrow action) {
        Utils.swallow(action, (e) -> logger.warn(e.getMessage(), e));
    }

    public void swallow(ActionWithThrow action) {
        swallowWarn(action);
    }

    public void error(String msg) {
        logger.error(msgWithLogIdent(msg));
    }

    public void error(Throwable e) {
        logger.error(logIdent, e);
    }

    public void error(String msg, Throwable e) {
        logger.error(msgWithLogIdent(msg), e);
    }
    public void swallowError(ActionWithThrow action) {
        Utils.swallow(action, (e) -> logger.error(e.getMessage(), e));
    }

//
//    def fatal(msg:=>String):Unit =
//
//    {
//        logger.fatal(msgWithLogIdent(msg))
//    }
//
//    def fatal(e:=>Throwable):Any =
//
//    {
//        logger.fatal(logIdent, e)
//    }
//
//    def fatal(msg:=>String, e:=>Throwable) =
//
//    {
//        logger.fatal(msgWithLogIdent(msg), e)
//    }
}
