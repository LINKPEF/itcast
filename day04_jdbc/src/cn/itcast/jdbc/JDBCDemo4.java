package cn.itcast.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * account表 删除一条信息
 * DML（data manipulation language）数据操纵语言：
 */
public class JDBCDemo4 {
    public static void main(String[] args) {
        Connection conn=null;
        Statement stmt=null;
        try {
            //1注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            //2获取连接对象
            conn= DriverManager.getConnection("jdbc:mysql:///db3?useSSL=false","root","root");
            //3.定义sql语句
            String sql="delete from account where id=3";
            //4.获取执行sql对象
            stmt=conn.createStatement();
            //5.执行sql语句
            int count=stmt.executeUpdate(sql);
            //6.处理结果
            System.out.println(count);
            if (count>0){
                System.out.println("删除成功");
            }else{
                System.out.println("删除失败");
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
