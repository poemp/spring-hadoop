package org.poem.config;

import org.poem.AbstractDataBaseInter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;


/**
 * 数据初始化
 *
 * @author Administrator
 */
@Component
public class HiveDataConfig {

    private static final Logger logger = LoggerFactory.getLogger(HiveDataConfig.class);

    @Value("${hive.url}")
    private String url;

    @Value("${hive.user}")
    private String userName;

    @Value("${hive.password}")
    private String password;

    @Value("${hive.driver-class-name}")
    private String driver;

    @Bean
    public DataBaseInter getDataBaseS() {
        logger.info(String.format("init hive databases - \n\t\turl:%s, " +
                "\n\t\tuserName:%s, " +
                "\n\t\tpassword:%s, " +
                "\n\t\tdrive:%s" , url, userName, password, driver));
        return new AbstractDataBaseInter(new DateBaseConfig() {
            @Override
            public String getDriveName() {
                return driver;
            }

            @Override
            public String getUserName() {
                return userName;
            }

            @Override
            public String getPassword() {
                return password;
            }

            @Override
            public String getUrl() {
                return url;
            }
        });
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }
}
