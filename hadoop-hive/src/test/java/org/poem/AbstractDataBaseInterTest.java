package org.poem;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.poem.config.HiveDataConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = HadoopHiveApplciation.class)
public class AbstractDataBaseInterTest {

    @Autowired
    HiveDataConfig hiveDataConfig;

    @Test
    public void getConnect() {

    }

    @Test
    public void getDateBaseName() {
        Connection connection =  hiveDataConfig.getDataBaseS().getConnect();
        PreparedStatement statement = null;
        try {
            String sql = "CREATE TABLE SORT_COLS_" + System.currentTimeMillis() +"(\n" +
                    "SD_ID int  NOT NULL \n" +
                    ")";
            System.out.println(sql);
            statement = connection.prepareStatement(sql);
            boolean r = statement.execute();
            System.out.println(r);

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if (statement != null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void close() {
    }
}