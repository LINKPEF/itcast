package cn.itcast.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class JdbcDemo1 {
    public static void main(String[] args) throws Exception {
        //1.导入驱动jar包
        //2.注册驱动
        Class.forName("com.mysql.jdbc.Driver");
        //3.获取数据库连接对象
        Connection conn =DriverManager.getConnection("jdbc:mysql://localhost:3306/db3?useSSL=false","root","root");
        //4.定义sql语句
        String sql="update account set balance=500 where id=1";
        //5.获取执行sql的对象，Statement
        Statement stmt=conn.createStatement();
        //6.执行sql语句
        int count=stmt.executeUpdate(sql);
        //7.处理结果
        System.out.println(count);
        //8.释放资源(不释放内存会变小，因为内存中会有垃圾注入不能清理
        stmt.close();
        conn.close();

    }
}
