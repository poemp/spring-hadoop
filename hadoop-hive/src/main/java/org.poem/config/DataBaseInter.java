package org.poem.config;


import java.io.Closeable;
import java.sql.Connection;

/**
 * 数据结构
 * 获取数据库
 * 并且获取数据库表
 *
 * @author Administrator
 */
public interface DataBaseInter extends Closeable {


    /**
     * 获取连接
     *
     * @return
     */
    Connection getConnect();

    /**
     * 获取数据库连接信息
     *
     * @return
     */
    public String getDateBaseName();
}
