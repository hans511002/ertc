package com.ery.ertc.estorm.po;

public class OutputFieldPO extends FieldPO{

    private long nodeId;

    /**
     * 被计算的字段【如果属于关联后字段，则字段可以是维度表的字段】
     * 维度字段如 META_DIM_ZONE.ZONE_NAME
     * 除了常用汇总，可以自定义计算，可以输入多个字段作为参数
     */
    private String calcFields;
    private String calcMethod;//计算方法NONE,COUNT,MAX,MIN,SUM,AVG,FUN(自定义，与DefCalcPO关联)

    public long getNodeId() {
        return nodeId;
    }

    public void setNodeId(long nodeId) {
        this.nodeId = nodeId;
    }

    public String getCalcFields() {
        return calcFields;
    }

    public void setCalcFields(String calcFields) {
        this.calcFields = calcFields;
    }

    public String getCalcMethod() {
        return calcMethod;
    }

    public void setCalcMethod(String calcMethod) {
        this.calcMethod = calcMethod;
    }
}
