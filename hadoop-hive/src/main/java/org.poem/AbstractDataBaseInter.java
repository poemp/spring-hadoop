package org.poem;

import org.poem.config.DataBaseInter;
import org.poem.config.DateBaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 数据结构
 * 获取数据库
 * 并且获取数据库表
 */
public  class AbstractDataBaseInter implements DataBaseInter {

    private static final Logger logger = LoggerFactory.getLogger(AbstractDataBaseInter.class);
    /**
     * 获取数据
     */
    private DateBaseConfig dateBaseConfig;

    private Connection connection = null;

    /**
     * 配置
     * @param dateBaseConfig
     */
    public AbstractDataBaseInter(DateBaseConfig dateBaseConfig) {
        Assert.notNull(dateBaseConfig, "配置不能为空");
        if (StringUtils.isEmpty(dateBaseConfig.getDriveName())){
            throw new IllegalArgumentException("DriveName Not Null");
        }
        if (StringUtils.isEmpty(dateBaseConfig.getUrl())){
            throw new IllegalArgumentException("Url  Not Null");
        }
        if (StringUtils.isEmpty(dateBaseConfig.getUserName())){
            throw new IllegalArgumentException("User Name Not Null");
        }
        this.dateBaseConfig = dateBaseConfig;
    }

    /**
     * @return
     */
    @Override
    public Connection getConnect() {
        try {
            Class.forName(dateBaseConfig.getDriveName());
            logger.info(String.format("init database info: url:%s, user:%s, password:%s",
                    dateBaseConfig.getUrl(),dateBaseConfig.getUserName(),dateBaseConfig.getPassword()));
            this.connection = DriverManager.getConnection(dateBaseConfig.getUrl(), dateBaseConfig.getUserName(), dateBaseConfig.getPassword());
            return connection;
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
        }
        if (this.connection == null){
            throw new IllegalArgumentException("The Connect Is Null.");
        }
        return connection;
    }

    /**
     * @return
     */
    @Override
    public String getDateBaseName() {
        String schema = null;
        try {
            schema = connection.getSchema();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
        }
        return schema;
    }

    /**
     * 必须强制提交，关闭链接
     *
     * @throws IOException
     */
    @PreDestroy
    @Override
    public void close() throws IOException {
        if (connection != null) {
            logger.info("Database - Closed Connection . ");
            try {
                connection.isClosed();
                connection.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                e.printStackTrace();
            }
        }
    }


    private Connection getConnection() {
        return connection;
    }

    private void setConnection(Connection connection) {
        this.connection = connection;
    }

    private DateBaseConfig getDateBaseConfig() {
        return dateBaseConfig;
    }

    private void setDateBaseConfig(DateBaseConfig dateBaseConfig) {
        this.dateBaseConfig = dateBaseConfig;
    }
}
