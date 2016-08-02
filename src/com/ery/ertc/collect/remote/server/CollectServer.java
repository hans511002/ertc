package com.ery.ertc.collect.remote.server;

import com.ery.ertc.collect.exception.CollectException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class CollectServer {
    protected Map<String,Map<String,AtomicInteger>> ClientNumMap = new HashMap<String, Map<String, AtomicInteger>>();//每个消息客户端连接数
    protected Map<String,ServiceReqHandler> reqHandlerMap = new HashMap<String, ServiceReqHandler>();
    public abstract void start();
    public abstract void stop();
    public void register(String topic,ServiceReqHandler reqHandler) {
        if(reqHandlerMap.containsKey(topic)){
            throw new CollectException("消息["+topic+"]服务已注册,不可重复,请保证msgTagName唯一!");
        }
        reqHandlerMap.put(topic,reqHandler);
        ClientNumMap.put(topic,new HashMap<String, AtomicInteger>());
    }

    public void unRegister(String topic){
        reqHandlerMap.remove(topic);
        ClientNumMap.remove(topic);
    }

    public Map<String, ServiceReqHandler> getReqHandlerMap() {
        return reqHandlerMap;
    }
}
