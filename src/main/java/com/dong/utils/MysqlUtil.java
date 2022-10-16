package com.dong.utils;

import com.alibaba.fastjson2.JSONObject;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MysqlUtil {

    private static final String DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    private static final String URL = "jdbc:mysql://127.0.0.1:3306/rt?characterEncoding=utf-8&useSSL=true";
    private static final String USER_NAME = "root";
    private static final String USER_PWD = "qwe123";

    public static JSONObject getList(String sql){
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Class.forName(DRIVER_NAME);
            conn = DriverManager.getConnection(URL, USER_NAME, USER_PWD);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            List<JSONObject> resultList = new ArrayList<>();
            JSONObject ret = new JSONObject();
            while (rs.next()){
                ret.put("id",rs.getInt("id"));
                ret.put("shop_name",rs.getString("shop_name"));
                ret.put("shop_id",rs.getInt("shop_id"));
                break;
            }
            return ret;
        } catch (Exception throwables) {
            throwables.printStackTrace();
            new RuntimeException("msql 查询失败！");
        } finally {
            if(rs!=null){
                try {
                    rs.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if(ps!=null){
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if(conn!=null){
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
        return null;
    }

    public static void main(String[] args) {
        JSONObject ret = MysqlUtil.getList("select * from shop_info where shop_name='阿萨德' limit 1;");
        System.out.println(String.format("ret={}",ret));
    }
}
