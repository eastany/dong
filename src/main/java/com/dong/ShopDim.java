package com.dong;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;

public class ShopDim extends RichParallelSourceFunction<Shop> {
    private PreparedStatement ps;
    private Connection connection;
    private volatile int lastUpdateMin = -1;
    private volatile boolean isRunning;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRunning = true;
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/shop?characterEncoding=UTF-8", "root", "qwe123");
        String sql="select * from shop_info";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }


    @Override
    public void run(SourceContext<Shop> sourceContext) throws Exception {
        try {
            while (isRunning) {
                LocalDateTime date = LocalDateTime.now();
                int min = date.getMinute();
                ResultSet rs = ps.executeQuery();
                if(min != lastUpdateMin) {
                    lastUpdateMin = min;
                    while (rs.next()) {
                        Shop s = new Shop();
                        sourceContext.collect(s);
                    }
                }
                Thread.sleep(1000 * 3600);
            }
        } catch (Exception e){
            System.out.println("Mysql data update error.."+e.getMessage());
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
    }

}
