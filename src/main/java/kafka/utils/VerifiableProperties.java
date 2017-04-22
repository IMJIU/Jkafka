package kafka.utils;

import com.google.common.collect.Sets;
import kafka.func.Handler;
import kafka.func.Tuple;
import kafka.message.CompressionCodec;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Administrator on 2017/4/3.
 */
public class VerifiableProperties extends Logging {
    public Properties props;
    private HashSet referenceSet = Sets.newHashSet();

    public VerifiableProperties(Properties props) {
        this.props = props;
    }

    public VerifiableProperties() {
        this.props = new Properties();
    }

    public Boolean containsKey(String name) {
        return props.containsKey(name);
    }

    public String getProperty(String name) {
        String value = props.getProperty(name);
        referenceSet.add(name);
        return value;
    }

    /**
     * Read a required integer property value or throw an exception if no such property is found
     */
    public Integer getInt(String name) {
        return Integer.parseInt(getString(name));
    }

    public Integer getIntInRange(String name, Tuple<Integer, Integer> range) {
        Prediction.require(containsKey(name), "Missing required property '" + name + "'");
        return getIntInRange(name, -1, range);
    }

    /**
     * Read an integer from the properties instance
     *
     * @param name The property name
     * @param def  The default value to use if the property is not found
     * @return the integer value
     */
    public Integer getInt(String name, Integer def) {
        return getIntInRange(name, def, Tuple.of(Integer.MIN_VALUE, Integer.MAX_VALUE));
    }

    public Short getShort(String name, Short def) {
        return getShortInRange(name, def, Tuple.of(Short.MIN_VALUE, Short.MAX_VALUE));
    }


    /**
     * Read an integer from the properties instance. Throw an exception
     * if the value is not in the given range (inclusive)
     *
     * @param name  The property name
     * @param def   The default value to use if the property is not found
     * @param range The range in which the value must fall (inclusive)
     * @return the integer value
     * @throws IllegalArgumentException If the value is not in the given range
     */
    public Integer getIntInRange(String name, Integer def, Tuple<Integer, Integer> range) {
        Integer v;
        if (containsKey(name))
            v = getInt(name);
        else
            v = def;
        Prediction.require(v >= range.v1 && v <= range.v2, name + " has value " + v + " which is not in the range " + range + ".");
        return v;
    }

    public Short getShortInRange(String name, Short def, Tuple<Short, Short> range) {
        Short v;
        if (containsKey(name))
            v = Short.parseShort(getProperty(name));
        else
            v = def;
        Prediction.require(v >= range.v1 && v <= range.v2, name + " has value " + v + " which is not in the range " + range + ".");
        return v;
    }

    /**
     * Read a required long property value or throw an exception if no such property is found
     */
    public Long getLong(String name) {
        return Long.parseLong(getString(name));
    }

    /**
     * Read an long from the properties instance
     *
     * @param name The property name
     * @param def  The default value to use if the property is not found
     * @return the long value
     */
    public Long getLong(String name, Long def) {
        return getLongInRange(name, def, Tuple.of(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    /**
     * Read an long from the properties instance. Throw an exception
     * if the value is not in the given range (inclusive)
     *
     * @param name  The property name
     * @param def   The default value to use if the property is not found
     * @param range The range in which the value must fall (inclusive)
     * @return the long value
     * @throws IllegalArgumentException If the value is not in the given range
     */
    public Long getLongInRange(String name, Long def, Tuple<Long, Long> range) {
        Long v;
        if (containsKey(name))
            v = getLong(name);
        else
            v = def;
        Prediction.require(v >= range.v1 && v <= range.v2, name + " has value " + v + " which is not in the range " + range + ".");
        return v;
    }

    /**
     * Get a required argument as a double
     *
     * @param name The property name
     * @return the value
     * @throws IllegalArgumentException If the given property is not present
     */
    public Double getDouble(String name) {
        return Double.parseDouble(getString(name));
    }

    /**
     * Get an optional argument as a double
     *
     * @param name The property name
     * @param def  The default value for the property if not present
     */
    public Double getDouble(String name, Double def) {
        if (containsKey(name))
            return getDouble(name);
        else
            return def;
    }

    /**
     * Read a boolean value from the properties instance
     *
     * @param name The property name
     * @param def  The default value to use if the property is not found
     * @return the boolean value
     */
    public Boolean getBoolean(String name, Boolean def) {
        if (!containsKey(name))
            return def;
        else {
            String v = getProperty(name);
            Prediction.require(v == "true" || v == "false", "Unacceptable value for property '" + name + "', boolean values must be either 'true' or 'false");
            return Boolean.parseBoolean(v);
        }
    }

    public Boolean getBoolean(String name) {
        return Boolean.parseBoolean(getString(name));
    }

    /**
     * Get a string property, or, if no such property is defined, return the given default value
     */
    public String getString(String name, String def) {
        if (containsKey(name))
            return getProperty(name);
        else
            return def;
    }

    /**
     * Get a string property or throw and exception if no such property is defined.
     */
    public String getString(String name) {
        Prediction.require(containsKey(name), "Missing required property '" + name + "'");
        return getProperty(name);
    }

    public Map<String, String> getMap(String name) {
        return getMap(name, s -> true);
    }

    /**
     * Get a Map<String, String> from a property list in the form v2 k1, v2 k2, ...
     */
    public Map<String, String> getMap(String name, Handler<String, Boolean> valid) {
        try {
            Map<String, String> m = Utils.parseCsvMap(getString(name, ""));
            // TODO: 2017/4/4     case (key,value)=>  if (!valid.process(value))
            m.forEach((key, value) -> {
                if (!valid.handle(key))
                    throw new IllegalArgumentException(String.format("Invalid entry '%s' = '%s' for property '%s'", key, value, name));
            });
            return m;
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Error parsing configuration property '%s': %s", name, e.getMessage()));
        }
    }

    /**
     * Parse compression codec from a property list in either. Codecs may be specified as integers, or as strings.
     * See <[kafka.message.CompressionCodec]> for more details.
     *
     * @param name The property name
     * @param def  Default compression codec
     * @return compression codec
     */
    // TODO: 2017/4/4  def unused
    public CompressionCodec getCompressionCodec(String name, CompressionCodec def) {
        String prop = getString(name, CompressionCodec.NoCompressionCodec.name);
        try {
            return CompressionCodec.getCompressionCodec(Integer.parseInt(prop));
        } catch (NumberFormatException nfe) {
            return CompressionCodec.getCompressionCodec(prop);
        }
    }

    public void verify() {
        info("Verifying properties");
        List<String> propNames = Collections.list(props.propertyNames()).stream()
                .map(p -> p.toString()).sorted().collect(Collectors.toList());

        for (String key : propNames) {
            if (!referenceSet.contains(key) && !key.startsWith("external"))
                warn(String.format("Property %s is not valid", key));
            else
                info(String.format("Property %s is overridden to %s", key, props.getProperty(key)));
        }
    }

    @Override
    public String toString() {
        return props.toString();
    }

}
