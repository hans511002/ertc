package com.ery.ertc.collect.handler.distinct;

public class Conf {
    public static final int DEFAULT_HASH_NUM = 3;
    public static final byte DEFAULT_MEM_BUCKET_CAP = 100;
    public static final int DEFAULT_BLOCK_BIT_LEN = 10;
    public static final int DEFAULT_CACHE_CYCLE = 5;//5秒

    //可选配置
    private int blockBitLen = DEFAULT_BLOCK_BIT_LEN;//桶对应的一个块大小 = 1<<10 = 2^10 = 1024

    //必选配置
    private int numLevel = 10000;//数量级 至少1万
    private byte memBucketCap;//内存桶容量，与内存限制2选1配置
    private double memLimit;//内存限制，单位mb。默认1mb
    private long fileCycle;//缓存文件周期。时间范围（毫秒）。一个小时为3600*1000毫秒。比如可以一个小时一个文件。5个小时一个文件等。
    private String filePath;//文件基础名

    private String busKey;//业务key.用于打印日志识别具体业务
    private int checkRange = 1;//去重检查文件范围,至少=1

    public int getNumLevel() {
        return numLevel;
    }

    public void setNumLevel(int numLevel) {
        if(numLevel>10000){
            this.numLevel = numLevel;
        }
    }

    public byte getMemBucketCap() {
        return memBucketCap;
    }

    public void setMemBucketCap(byte memBucketCap) {
        this.memBucketCap = memBucketCap;
    }

    public int getBlockBitLen() {
        return blockBitLen;
    }

    public void setBlockBitLen(int blockBitLen) {
        if(blockBitLen>DEFAULT_BLOCK_BIT_LEN){
            this.blockBitLen = blockBitLen;
        }
    }

    public double getMemLimit() {
        return memLimit;
    }

    public void setMemLimit(double memLimit) {
        this.memLimit = memLimit;
    }

    public long getFileCycle() {
        return fileCycle;
    }

    public void setFileCycle(long fileCycle) {
        this.fileCycle = fileCycle;
    }

    public int getCheckRange() {
        return checkRange;
    }

    public void setCheckRange(int checkRange) {
        if(checkRange>1){
            this.checkRange = checkRange;
        }
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getBusKey() {
        return busKey;
    }

    public void setBusKey(String busKey) {
        this.busKey = busKey;
    }
}
