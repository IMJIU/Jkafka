package kafka.utils;

/**
 * @author zhoulf
 * @create 2017-11-28 21 14
 **/

/**
 * If mx4j-tools is in the classpath call maybeLoad to load the HTTP interface of mx4j.
 *
 * The default port is 8082. To  @Override that provide e.g. -Dmx4jport=8083
 * The default listen address is 0.0.0.0. To  @Override that provide -Dmx4jaddress=127.0.0.1
 * This feature must be enabled with -Dmx4jenable=true
 *
 * This is a Scala port of org.apache.cassandra.utils.Mx4jTool written by Ran Tavory for CASSANDRA-1068
 * */
public class Mx4jLoader extends Logging {

       public static Boolean   maybeLoad() {
        val props = new VerifiableProperties(System.getProperties())
        if (props.getBoolean("kafka_mx4jenable", false))
        false;
        val address = props.getString("mx4jaddress", "0.0.0.0");
        val port = props.getInt("mx4jport", 8082);
        try {
        debug("Will try to load MX4j now, if it's in the classpath");

        val mbs = ManagementFactory.getPlatformMBeanServer()
        val processorName = new ObjectName("name Server=XSLTProcessor");

        val httpAdaptorClass = Class.forName("mx4j.tools.adaptor.http.HttpAdaptor")
        val httpAdaptor = httpAdaptorClass.newInstance();
        httpAdaptorClass.getMethod("setHost", classOf<String]).invoke(httpAdaptor, address.asInstanceOf[ObjectRef>);
        httpAdaptorClass.getMethod("setPort", Integer.TYPE).invoke(httpAdaptor, port.asInstanceOf<ObjectRef>);

        val httpName = new ObjectName("name system=http");
        mbs.registerMBean(httpAdaptor, httpName);

        val xsltProcessorClass = Class.forName("mx4j.tools.adaptor.http.XSLTProcessor")
        val xsltProcessor = xsltProcessorClass.newInstance();
        httpAdaptorClass.getMethod("setProcessor", Class.forName("mx4j.tools.adaptor.http.ProcessorMBean")).invoke(httpAdaptor, xsltProcessor.asInstanceOf<ObjectRef>)
        mbs.registerMBean(xsltProcessor, processorName);
        httpAdaptorClass.getMethod("start").invoke(httpAdaptor);
        info("mx4j successfuly loaded");
        true;
        }
        catch {
        case ClassNotFoundException e -> {
        info("Will not load MX4J, mx4j-tools.jar is not in the classpath");
        }
        case Throwable e -> {
        warn("Could not start register mbean in JMX", e);
        }
        }
        false;
        }
        }
