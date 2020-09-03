package cn.itcast.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * account表 添加一条语句，
 */
public class JdbcDemo2 {
    public static void main(String[] args) {
        Statement stmt=null;
        Connection conn=null;
        try {
            //1.注册驱动(这里try catch 不再自动抛异常）
            Class.forName("com.mysql.jdbc.Driver");
            //2.定义sql
            String sql ="insert into account values(null,'wangwu',3000)";
            //3.读取Connection对象(Alt+enter)
           conn =DriverManager.getConnection("jdbc:mysql:///db3?useSSL=false","root","root");
            //4.获取执行sql的对象
            stmt = conn.createStatement();
            //5.执行sql
            int count=stmt.executeUpdate(sql);//影响的行数
            //6.处理结果
            System.out.println(count);
            if (count>0){
                System.out.println("添加成功");
            }else{
                System.out.println("添加失败");
            }
        } catch (ClassNotFoundException e) {
           e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            //stmt在try作用域，这里会报红，我们可以在外面定义
            /*stmt.close();在这里使用stmt.close()方法是不对的，因为前面获取connection可能会报错
            报错后就不会执行后面的语句，此时会空指针异常*/
            //7.释放资源，避免空指针异常
            if(stmt!=null){
                try {
                    stmt.close();
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
