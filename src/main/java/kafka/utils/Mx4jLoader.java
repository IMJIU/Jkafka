package kafka.utils;

/**
 * @author zhoulf
 * @create 2017-11-28 21 14
 **/

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * If mx4j-tools is in the classpath call maybeLoad to load the HTTP interface of mx4j.
 * <p>
 * The default port is 8082. To  @Override that provide e.g. -Dmx4jport=8083
 * The default listen address is 0.0.0.0. To  @Override that provide -Dmx4jaddress=127.0.0.1
 * This feature must be enabled with -Dmx4jenable=true
 * <p>
 * This is a Scala port of org.apache.cassandra.utils.Mx4jTool written by Ran Tavory for CASSANDRA-1068
 */
public class Mx4jLoader {
    private static Logger logger = LoggerFactory.getLogger(Mx4jLoader.class);
    public static Boolean maybeLoad() {
        VerifiableProperties props = new VerifiableProperties(System.getProperties());
        if (props.getBoolean("kafka_mx4jenable", false))
            return false;
        String address = props.getString("mx4jaddress", "0.0.0.0");
        int port = props.getInt("mx4jport", 8082);
        try {
            logger.debug("Will try to load MX4j now, if it's in the classpath");

            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName processorName = new ObjectName("Server:name=XSLTProcessor");

            Class httpAdaptorClass = null;

            httpAdaptorClass = Class.forName("mx4j.tools.adaptor.http.HttpAdaptor");

            Object httpAdaptor = httpAdaptorClass.newInstance();
            httpAdaptorClass.getMethod("setHost", String.class).invoke(httpAdaptor, address);
            httpAdaptorClass.getMethod("setPort", Integer.TYPE).invoke(httpAdaptor, port);

            ObjectName httpName = new ObjectName("name system=http");
            mbs.registerMBean(httpAdaptor, httpName);

            Class xsltProcessorClass = Class.forName("mx4j.tools.adaptor.http.XSLTProcessor");
            Object xsltProcessor = xsltProcessorClass.newInstance();
            httpAdaptorClass.getMethod("setProcessor", Class.forName("mx4j.tools.adaptor.http.ProcessorMBean")).invoke(httpAdaptor, xsltProcessor);
            mbs.registerMBean(xsltProcessor, processorName);
            httpAdaptorClass.getMethod("start").invoke(httpAdaptor);
            logger.info("mx4j successfuly loaded");
            return true;
        } catch (ClassNotFoundException e) {
            logger.info("Will not load MX4J, mx4j-tools.jar is not in the classpath");
        } catch (Throwable e) {
            logger.warn("Could not start register mbean in JMX", e);
        }
        return false;
    }
}
