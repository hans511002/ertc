package com.ery.ertc.collect.forwards;

import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.sys.podo.BaseDAO;

public class CacheDAO extends BaseDAO{

    //尝试建立缓存表
    public void tryCreateCacheTbl(String dsId,String crtTblSql){
        try{
            getDataAccess(dsId).execNoQuerySql(crtTblSql);
        }catch (Exception e){
            LogUtils.warn(null, e);
        }
    }

    //获取最大最小缓存记录
    public Long[] getMinAndMaxCacheItem(String dsId, String querySql){
        return getDataAccess(dsId).queryForBeanArray(querySql,Long.class);
    }
    
}
