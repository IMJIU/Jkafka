package kafka.utils;/**
 * Created by zhoulf on 2017/3/23.
 */

import org.apache.log4j.Logger;

/**
 * @author
 * @create 2017-03-23 16:33
 **/
public class Logging {
    public String loggerName = this.getClass().getName();
    public Logger logger = Logger.getLogger(loggerName);

    protected String logIdent = null;

    // Force initialization to register Log4jControllerMBean
    private Log4jController log4jController;

    //
    private String msgWithLogIdent(String msg) {
        if (logIdent == null) {
            return msg;
        } else {
            return logIdent + msg;
        }
    }

    //
    public void trace(String msg) {
        if (logger.isTraceEnabled())
            logger.trace(msgWithLogIdent(msg));
    }

    //
//    def trace(e:=>Throwable):Any =
//
//    {
//        if (logger.isTraceEnabled())
//            logger.trace(logIdent, e)
//    }
//
//    def trace(msg:=>String, e:=>Throwable) =
//
//    {
//        if (logger.isTraceEnabled())
//            logger.trace(msgWithLogIdent(msg), e)
//    }
//
//    def swallowTrace(action:=>Unit) {
//        Utils.swallow(logger.trace, action)
//    }
//
    public void debug(String msg) {
        if (logger.isDebugEnabled())
            logger.debug(msgWithLogIdent(msg));
    }

    //
//    def debug(e:=>Throwable):Any =
//
//    {
//        if (logger.isDebugEnabled())
//            logger.debug(logIdent, e)
//    }
//
//    def debug(msg:=>String, e:=>Throwable) =
//
//    {
//        if (logger.isDebugEnabled())
//            logger.debug(msgWithLogIdent(msg), e)
//    }
//
//    def swallowDebug(action:=>Unit) {
//        Utils.swallow(logger.debug, action)
//    }
//
    public void info(String msg) {
        if (logger.isInfoEnabled())
            logger.info(msgWithLogIdent(msg));
    }

    //
//    def info(e:=>Throwable):Any =
//
//    {
//        if (logger.isInfoEnabled())
//            logger.info(logIdent, e)
//    }
//
//    def info(msg:=>String, e:=>Throwable) =
//
//    {
//        if (logger.isInfoEnabled())
//            logger.info(msgWithLogIdent(msg), e)
//    }
//
//    def swallowInfo(action:=>Unit) {
//        Utils.swallow(logger.info, action)
//    }
//
    public void warn(String msg) {
        logger.warn(msgWithLogIdent(msg));
    }

    //
//    def warn(e:=>Throwable):Any =
//
//    {
//        logger.warn(logIdent, e)
//    }
//
    public void warn(String msg, Throwable e) {
        logger.warn(msgWithLogIdent(msg), e);
    }

    //
//    def swallowWarn(action:=>Unit) {
//        Utils.swallow(logger.warn, action)
//    }
//
//    def swallow(action:=>Unit) =
//
//    swallowWarn(action)
//
    public void error(String msg) {
        logger.error(msgWithLogIdent(msg));
    }

    //
//    def error(e:=>Throwable):Any =
//
//    {
//        logger.error(logIdent, e)
//    }
//
    public void error(String msg, Throwable e) {
        logger.error(msgWithLogIdent(msg), e);
    }
//
//    def swallowError(action:=>Unit) {
//        Utils.swallow(logger.error, action)
//    }
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
