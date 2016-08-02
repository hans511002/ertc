package com.ery.ertc.collect.zk;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.io.UnsupportedEncodingException;

public class StringZkSerializer implements ZkSerializer{
    
    private String charset;

    public StringZkSerializer() {
    }

    public StringZkSerializer(String charset) {
        this.charset = charset;
    }

    @Override
    public byte[] serialize(Object o) throws ZkMarshallingError {
        if(charset!=null && !"".equals(charset)){
            try {
                o.toString().getBytes(charset);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return o.toString().getBytes();
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        if(charset!=null && !"".equals(charset)){
            try {
                return new String(bytes,charset);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return new String(bytes);
    }
}
