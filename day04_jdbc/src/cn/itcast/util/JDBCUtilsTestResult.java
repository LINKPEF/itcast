package cn.itcast.util;

import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.*;
import java.util.Properties;

/***
 *
 * JDBC工具类
 * 工具类所有的方法一般是静态类
 * 如果路径中存在空格，则会报如下错误，
 * https://blog.csdn.net/qq_38454176/article/details/104101972
 */
public class JDBCUtilsTestResult {
    /**
     * 3.使用配置文件：第十四行代码开始
     * 文件的读取，只需要读取一次即可拿到这些值，使用静态代码块
     */
    private static String url;
    private static String user;
    private static String password;
    private static String driver;
    /*
    静态代码块里面只能处理异常，不能抛，抛异常需要借用方法
     */
    static {
        //读取资源文件，获取值
        try {
            //1.创建Properties集合类。
            Properties pro=new Properties();

            //2.加载文件
            /*java.io.FileNotFoundException: src\standarMmd.properties (系统找不到指定的路径。)
            原因是我们写的是相对路径，应该写个绝对路径，但是写绝对路径我们修改路径后这里也需要更改*/
            //pro.load(new FileReader("src/standarMmd.properties"));

            //2.通过获取src路径下文件的方式---》ClassLoader(类加载器）,URL是统一资源标识定位符

           /* 类路径出现错误的时候说明这个地方类路径存在转义后空格
           ClassLoader classLoader=JDBCUtils.class.getClassLoader();
           URL res=classLoader.getResource("standarMmd.properties");
            String path=res.getPath();
            System.out.println(path);*/
            ClassLoader classLoader = JDBCUtilsTestResult.class.getClassLoader();
            URL res  = classLoader.getResource("TestResult.properties");
            String path = res.toURI().getPath();//URL对象转换成字符串前，先调用toURI()方法
            System.out.println(path);
            // pro.load(JDBCUtils.class.getResourceAsStream("standarMmd.properties"));
            pro.load(new FileReader(path));
            //3.获取属性赋值
            url=pro.getProperty("url");
            user=pro.getProperty("user");
            password=pro.getProperty("password");
            driver=pro.getProperty("driver");
            //4.注册驱动
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接
     * @return 连接对象
     */
    public static Connection getConnection() throws SQLException {
        /*1.不传参情况：
        如果还按JDBCDemo8那种参数则需要经常改变，把工具类传参，不写死,
        return DriverManager.getConnection("jdbc:mysql:///db3?useSSL=false","root","root");
        */

        /*2.传参情况：
        * 其实这种情况也不好，直接在其他类里面调用工具类和直接写方法没太大区别
        *
        * public static Connection getConnection(String url,String user,String password) throws SQLException{
        return  DriverManager.getConnection(url,user,password);}
        */
        return  DriverManager.getConnection(url,user,password);
    }

    /**
     * 释放资源的方法1
     */
    public static void close(Statement stmt,Connection conn){
        if (stmt!=null){
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
    /**
     * 释放资源的方法2
     * 重载机制
     */
    public static void close(ResultSet rs,Statement stmt, Connection conn){
        if (rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (stmt!=null){
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
