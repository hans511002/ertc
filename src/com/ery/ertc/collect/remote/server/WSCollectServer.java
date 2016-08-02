package com.ery.ertc.collect.remote.server;

import com.ery.ertc.collect.conf.Config;
import com.ery.ertc.collect.zk.NodeTracker;
import com.ery.ertc.collect.zk.ZkConstant;
import com.ery.base.support.log4j.LogUtils;

import javax.xml.ws.Endpoint;

public class WSCollectServer extends CollectServer{

    private CollectWS collectWS;
    private boolean started;

    public WSCollectServer() {
        collectWS = new CollectWS();
        collectWS.setWsServer(this);
    }

    @Override
    public void start() {
        if(started){
            return;
        }
        String wsurl = "http://"+Config.getHostName()+":"+ Config.getServerWSPort() +"/CollectWS?wsdl";
        try{
            Endpoint.publish(wsurl, collectWS);
            started = true;
            NodeTracker.noticeTryNetServer(ZkConstant.NODE_WS_PORT,Config.getServerWSPort());
            LogUtils.info("消息接收WebService服务[" + wsurl + "]启动 OK!");
        }catch (Exception e){
            started = false;
            NodeTracker.noticeTryNetServer(ZkConstant.NODE_WS_PORT,e.getMessage());
            LogUtils.error("启动WS服务["+wsurl+"]出错!:"+e.getMessage(),e);
        }
    }

    @Override
    public void stop() {
        collectWS = null;
        started = false;
        String wsurl = "http://"+Config.getHostName()+":"+ Config.getServerWSPort() +"/CollectWS?wsdl";
        LogUtils.info("消息接收WebService服务[" + wsurl + "]停止 OK!");
    }

    public boolean isStarted(){
        return started;
    }
}
