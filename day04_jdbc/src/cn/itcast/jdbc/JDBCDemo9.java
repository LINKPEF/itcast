package cn.itcast.jdbc;

import cn.itcast.util.JDBCUtilsPmcis;

import java.sql.*;
import java.util.Scanner;

/*sql注入问题：
请输入用户名
123
请输入密码
a' or 'a' = 'a
/C:/Users/123/Documents/Tencent Files/755607262/FileRecv/itcast/out/production/day04_jdbc/mcm.properties
登录成功
1.通过键盘录入用户名和密码
2.判断用户是否登入成功
* */
public class JDBCDemo9 {
    public static void main(String[] args) {
        //1.键盘录入，接收用户名和密码
        Scanner sc=new Scanner(System.in);
        System.out.println("请输入用户名");
        String username = sc.nextLine();
        System.out.println("请输入密码");
        String password = sc.nextLine();
        //2.调用方法
        boolean flag=new JDBCDemo9().login2(username,password);
        //3.判断结果，输出不同语句
        if (flag){
            //登录成功
            System.out.println("登录成功");
        }else{
            System.out.println("用户名或密码错误");
        }
    }
    /*
    * 登入方法
    * */
    public boolean login(String username,String password){
        if (username==null||password==null){
            return false;
        }
        //连接数据库判断是否登入成功
        Connection conn=null;
        Statement stmt=null;
        ResultSet rs=null;
        //1.获取连接
        try {
             conn= JDBCUtilsPmcis.getConnection();
            //2.定义sql
            String sql="select * from user where username= '"+username+"'and password ='"+password+"'";
            //3.获取执行sql的对象
             stmt=conn.createStatement();
            //4.执行查询
             rs=stmt.executeQuery(sql);
            //5.判断
            //不建议这样写
          /*  if(rs.next()){//如果有下一行，则返回true
                return true;
            }else{
                return false;
            }*/
            return rs.next();//如果有下一行，则返回true
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JDBCUtilsPmcis.close(rs,stmt,conn);
        }

        return false;
    }
    /*
     * 登入方法：使用PreparedStatement实现
     * */
    public boolean login2(String username,String password){
        if (username==null||password==null){
            return false;
        }
        //连接数据库判断是否登入成功
        Connection conn=null;
        PreparedStatement  pstmt=null;
        ResultSet rs=null;
        //1.获取连接
        try {
            conn= JDBCUtilsPmcis.getConnection();
            //2.定义sql
            String sql="select * from user where username=? and password =?";
          /*  //3.获取执行sql的对象
            stmt=conn.createStatement();*/
          pstmt=conn.prepareStatement(sql);
          //给？赋值的操作
            pstmt.setString(1,username);
            pstmt.setString(2,password);
            //4.执行查询:这个地方不需要传参
            rs=pstmt.executeQuery();
            //5.判断
            //不建议这样写
          /*  if(rs.next()){//如果有下一行，则返回true
                return true;
            }else{
                return false;
            }*/
            return rs.next();//如果有下一行，则返回true
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JDBCUtilsPmcis.close(rs,pstmt,conn);
        }

        return false;
    }
}
