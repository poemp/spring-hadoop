package org.poem.config;

/**
 * @author Administrator
 */
public interface DateBaseConfig {

    /**
     * 驱动
     *
     * @return
     */
    String getDriveName();

    /**
     * 获取数据
     *
     * @return
     */
    String getUserName();

    /**
     * 密码
     *
     * @return
     */
    String getPassword();

    /**
     * 数据库连接
     *
     * @return
     */
    String getUrl();


}
