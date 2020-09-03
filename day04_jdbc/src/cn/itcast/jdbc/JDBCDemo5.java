package cn.itcast.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 执行DDL（data definition language）数据库定义语言语句,
 * 没有返回结果所以int count=stmt.executeUpdate(sql);删除了
 */
public class JDBCDemo5 {
    public static void main(String[] args) {
        Connection conn=null;
        Statement stmt=null;
        try {
            //1注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            //2获取连接对象
            conn= DriverManager.getConnection("jdbc:mysql:///db3?useSSL=false","root","root");
            //3.定义sql语句
            String sql="create table student(id int,name varchar(20))";
            //4.获取执行sql对象
            stmt=conn.createStatement();
            //5.执行sql语句
            int count=stmt.executeUpdate(sql);//executeUpdate()一般执行DML语句
          /*  //6.处理结果
            System.out.println(count);
            if (count>0){
                System.out.println("删除成功");
            }else{
                System.out.println("删除失败");
            }*/
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
