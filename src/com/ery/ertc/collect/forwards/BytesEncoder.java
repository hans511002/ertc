package com.ery.ertc.collect.forwards;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import java.io.Serializable;

public class BytesEncoder implements Encoder<byte[]>,Serializable {

    public BytesEncoder(VerifiableProperties verifiableProperties) {
    }

    @Override
    public byte[] toBytes(byte[] bytes) {
        return bytes;
    }
}
