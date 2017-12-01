package kafka.serializer;

import kafka.utils.VerifiableProperties;

import java.io.UnsupportedEncodingException;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


public interface Encoder<T> {

    /**
     * An encoder is a method of turning objects into byte arrays.
     * An implementation is required to provide a constructor that
     * takes a VerifiableProperties instance.
     */
    byte[] toBytes(T t);
}

/**
 * The default implementation is a no-op, it just returns the same array it takes in
 */
class DefaultEncoder implements Encoder<byte[]> {
    public VerifiableProperties props = null;

    public DefaultEncoder(VerifiableProperties props) {
        this.props = props;
    }

    @Override
    public byte[] toBytes(byte[] value) {
        return value;
    }
}

class NullEncoder<T> implements Encoder<T> {
    VerifiableProperties props = null;

    public NullEncoder(VerifiableProperties props) {
        this.props = props;
    }

    @Override
    public byte[] toBytes(T value) {
        return null;
    }
}

/**
 * The string encoder takes an optional parameter serializer.encoding which controls
 * the character set used in encoding the string into bytes.
 */
class StringEncoder implements Encoder<String> {
    VerifiableProperties props = null;

    public StringEncoder(VerifiableProperties props) {
        this.props = props;
        if (props == null)
            encoding = "UTF8";
        else
            encoding = props.getString("serializer.encoding", "UTF8");
    }

    String encoding;

    @Override
    public byte[] toBytes(String s) {
        if (s == null)
            return null;
        else
            try {
                return s.getBytes(encoding);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
    }
}

