package com.ery.ertc.collect.ui.servlet;

import com.ery.ertc.collect.DaemonMaster;
import com.ery.ertc.collect.remote.server.ServiceReqHandler;
import org.I0Itec.zkclient.ZkClient;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class NodeInfoServlet extends ServiceReqHandler {

    private DaemonMaster daemonMaster;
    private ZkClient zkClient;

    public NodeInfoServlet(DaemonMaster daemonMaster) {
        this.daemonMaster = daemonMaster;
        zkClient = daemonMaster.getZkClient();
    }

    @Override
    public String doHttp(HttpServletRequest request, HttpServletResponse response) {
        StringBuilder str = new StringBuilder();
        str.append("节点ID:"+request.getParameter("nodeId"));
        return str.toString();
    }

}
