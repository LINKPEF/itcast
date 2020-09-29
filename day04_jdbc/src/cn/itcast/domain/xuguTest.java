package cn.itcast.domain;

import cn.itcast.util.XGJDBCUtilsMdm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 测试连接虚谷数据库
 */
public class xuguTest {
    public static void main(String[] args) throws SQLException {
        Connection connection = XGJDBCUtilsMdm.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from ACODE");
        while(resultSet.next())
            System.out.println(resultSet.getString(1));
    }
}
