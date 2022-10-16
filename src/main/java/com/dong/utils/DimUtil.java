package com.dong.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

@Slf4j
public class DimUtil {
    public static JSONObject getDimInfo(String tableName, String shopName) {
        String redisKey = "dim:"+tableName+":"+shopName;
        Jedis jedis = null;
        String redisStr = null;
        JSONObject dimInfoJsonObj = null;
        try {
            jedis = RedisUtil.getJedis();
            redisStr = jedis.get(redisKey);
            dimInfoJsonObj = null;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询redis数据错误！");
        }
        //组合查询 SQL
        String sql = "select * from " + tableName + " where shop_name = '"+shopName+"'";
        log.debug("查询维度 SQL:{}",sql);

        if(redisStr!=null && redisStr.length()>0){
            dimInfoJsonObj = JSON.parseObject(redisStr);
        }else {
            JSONObject dimList = MysqlUtil.getList(sql);
            if (dimList != null && dimList.size() > 0) {
                //因为关联维度，肯定都是根据 key 关联得到一条记录
                dimInfoJsonObj = dimList;
                if(jedis!=null){
                    jedis.setex(redisKey,3600*24,dimInfoJsonObj.toString());
                }
            }else{
                log.warn("维度数据未找到:{}", sql);
            }
        }
        //关闭jedis
        if(jedis!=null){
            jedis.close();
        }

        return dimInfoJsonObj;
    }

//    public static JSONObject getDimInfoNoCacheById(String tableName, String idValue) {
//        return getDimInfoNoCache(tableName,new Tuple2<>("id",idValue));
//    }
//    //直接从 Phoenix 查询，没有缓存
//    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>...
//            colNameAndValue) {
//        //组合查询条件
//        String wheresql = new String(" where ");
//        for (int i = 0; i < colNameAndValue.length; i++) {
//            //获取查询列名以及对应的值
//            Tuple2<String, String> nameValueTuple = colNameAndValue[i];
//            String fieldName = nameValueTuple.f0;
//            String fieldValue = nameValueTuple.f1;
//            if (i > 0) {
//                wheresql += " and ";
//            }
//            wheresql += fieldName + "='" + fieldValue + "'";
//        }
//        //组合查询 SQL
//        String sql = "select * from " + tableName + wheresql;
//        System.out.println("查询维度 SQL:" + sql);
//        JSONObject dimInfoJsonObj = null;
//        List<JSONObject> dimList = MysqlUtil.getList(sql);
//        if (dimList != null && dimList.size() > 0) {
//            //因为关联维度，肯定都是根据 key 关联得到一条记录
//            dimInfoJsonObj = dimList.get(0);
//        }else{
//            System.out.println("维度数据未找到:" + sql);
//        }
//        return dimInfoJsonObj;
//    }

    //根据 key 让 Redis 中的缓存失效
    public static void deleteCached( String tableName, String id){
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            // 通过 key 清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            log.warn("缓存异常！");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        JSONObject dimInfooNoCache = DimUtil.getDimInfoNoCache("base_trademark", Tuple2.of("id", "13"));
//        JSONObject dimInfooNoCache = DimUtil.getDimInfo("base_trademark", Tuple2.of("id", "13"));
//        System.out.println(dimInfooNoCache);
    }
}
