package com.ery.ertc.estorm.po;

import java.io.Serializable;
import java.util.List;

public class NodePO implements Serializable{

    //基本信息
    private long NODE_ID;//节点ID
    private String NODE_NAME;//节点名称
    private String NODE_DESC;//节点描述
    private int MAX_PARALLELISM_NUM;//最大并行度（工作的机器上运行此节点的实例数）
    private int MAX_PENDING;//最大等待数
    private boolean IS_DEBUG;//是否测试调试，默认false
    private boolean LOG_LEVEL;//记录日志级别，对应【POConstant.NODE_LOG_LEVEL_?】
    private List<NodeInputPO> nodeInputs;//节点输入
    private List<NodeOutputPO> nodeOutputs;//节点输出
    private String OUTPUT_FILTER;//输出过滤表达式
    private List<NodeJoinPO> nodeJoins;//节点关联
    private List<NodeStorePO> nodeStores;//节点存储

    public long getNODE_ID() {
        return NODE_ID;
    }

    public void setNODE_ID(long NODE_ID) {
        this.NODE_ID = NODE_ID;
    }

    public String getNODE_NAME() {
        return NODE_NAME;
    }

    public void setNODE_NAME(String NODE_NAME) {
        this.NODE_NAME = NODE_NAME;
    }

    public String getNODE_DESC() {
        return NODE_DESC;
    }

    public void setNODE_DESC(String NODE_DESC) {
        this.NODE_DESC = NODE_DESC;
    }

    public int getMAX_PARALLELISM_NUM() {
        return MAX_PARALLELISM_NUM;
    }

    public void setMAX_PARALLELISM_NUM(int MAX_PARALLELISM_NUM) {
        this.MAX_PARALLELISM_NUM = MAX_PARALLELISM_NUM;
    }

    public int getMAX_PENDING() {
        return MAX_PENDING;
    }

    public void setMAX_PENDING(int MAX_PENDING) {
        this.MAX_PENDING = MAX_PENDING;
    }

    public boolean isIS_DEBUG() {
        return IS_DEBUG;
    }

    public void setIS_DEBUG(boolean IS_DEBUG) {
        this.IS_DEBUG = IS_DEBUG;
    }

    public boolean isLOG_LEVEL() {
        return LOG_LEVEL;
    }

    public void setLOG_LEVEL(boolean LOG_LEVEL) {
        this.LOG_LEVEL = LOG_LEVEL;
    }

    public List<NodeInputPO> getNodeInputs() {
        return nodeInputs;
    }

    public void setNodeInputs(List<NodeInputPO> nodeInputs) {
        this.nodeInputs = nodeInputs;
    }

    public List<NodeOutputPO> getNodeOutputs() {
        return nodeOutputs;
    }

    public void setNodeOutputs(List<NodeOutputPO> nodeOutputs) {
        this.nodeOutputs = nodeOutputs;
    }

    public String getOUTPUT_FILTER() {
        return OUTPUT_FILTER;
    }

    public void setOUTPUT_FILTER(String OUTPUT_FILTER) {
        this.OUTPUT_FILTER = OUTPUT_FILTER;
    }

    public List<NodeJoinPO> getNodeJoins() {
        return nodeJoins;
    }

    public void setNodeJoins(List<NodeJoinPO> nodeJoins) {
        this.nodeJoins = nodeJoins;
    }

    public List<NodeStorePO> getNodeStores() {
        return nodeStores;
    }

    public void setNodeStores(List<NodeStorePO> nodeStores) {
        this.nodeStores = nodeStores;
    }
}
