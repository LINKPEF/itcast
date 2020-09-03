package cn.itcast.jdbc;

import java.sql.*;

/**
 * 执行DDL（data definition language）数据库定义语言语句,
 * 没有返回结果所以int count=stmt.executeUpdate(sql);删除了
 */
public class JDBCDemo6 {
    public static void main(String[] args) {
        Connection conn=null;
        Statement stmt=null;
        ResultSet rs=null;
        try {
            //1注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            //2获取连接对象
            conn= DriverManager.getConnection("jdbc:mysql://localhost:3306/pmcis?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC","root","root");
            //3.定义sql语句
            String sql="select * from jbpm4_property";
            //4.获取执行sql对象
            stmt=conn.createStatement();
            //5.执行sql语句
           // int count=stmt.executeUpdate(sql);//executeUpdate()一般执行DML语句
           rs=stmt.executeQuery(sql);//返回的是结果集
         //6.处理结果
            //6.1让游标向下移动一行
            rs.next();
            //6.2获取数据
           /* int id=rs.getInt(1);
            String name=rs.getString("name");
            double balance=rs.getDouble(3);
            System.out.println(id+"---"+name+"---"+balance);
            System.out.println(rs.next());*/
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            //7.释放资源
            if(rs!=null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn!=null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
