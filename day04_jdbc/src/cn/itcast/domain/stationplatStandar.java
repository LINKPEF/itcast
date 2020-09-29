package cn.itcast.domain;

import cn.itcast.util.JDBCUtilsPmcis;
import cn.itcast.util.JDBCUtilsTestResult;
import cn.itcast.util.XGMmdStandar;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import static java.lang.System.exit;

/**
 * 台站表的比较工具类
 */
public class stationplatStandar {
    static Statement stmtMdm = null;
    static Statement stmtPmcis = null;
    static Statement stmtTestResult = null;
    static StringBuilder insertSql;

    public static void main(String[] args) {
        try {
            stationplatMmdMain();
//            stationplatPmcisMain();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 查出MMD库的数据 再查询pmcis的数据进行比较
     * @throws Exception
     */
    public static void stationplatMmdMain() throws Exception {
        Connection connectionMmd = XGMmdStandar.getConnection();
        Connection connectionPmcis = JDBCUtilsPmcis.getConnection();
        Connection connectionTestResult = JDBCUtilsTestResult.getConnection();
        stmtMdm = connectionMmd.createStatement();
        stmtPmcis = connectionPmcis.createStatement();
        stmtTestResult = connectionTestResult.createStatement();
        insertSql = new StringBuilder("");
        //查找台站数据  先限制返回条数不然测试时间太长
        ResultSet resultMmdStation = stmtMdm.executeQuery("SELECT * FROM tab_omin_cm_cc_stationplat limit 30000  offset  120000 ");
        //
        int j = 0;
        while (resultMmdStation.next()) {
            System.out.println(++j);
            //获取每条记录的区站号
            String c_indexnbr = resultMmdStation.getString("c_indexnbr");
            System.out.println("SELECT * FROM tab_omin_cm_cc_stationplat where" +
                    "C_INDEXNBR = '" + c_indexnbr + "'");
            //       比较台站主表数据，台站号，站名，子站号，省市县代码，国家代码，行政区划C_ADMINISTRATIVEDC 。国家省市县名称字段统一update更改。
            //       查找PMCIS台站数据
            ResultSet resultPmcisStation = stmtPmcis.executeQuery("SELECT * FROM tab_omin_cm_cc_stationplat where " +
                    "C_INDEXNBR ='" + c_indexnbr + "'");
            //转到最后一行记录
            resultPmcisStation.last();
            int row = resultPmcisStation.getRow();
            if (row > 1) {
                System.out.println("记录大于1");
                // 如果返回多个记录，表示台站号相同，但是子站号不同，或者国家代码不同，导出数据。
                //先导出mmd的一条记录
                insertSql.append("insert into onetomaney_stationplat_mmd values(null,'mmd',");
                for (int i = 1; i < 97; i++) {
                    //偶尔会出现 a'b这样的数据
                    if (i == 12) {
                        String str1 = resultMmdStation.getString(i);
                        if (str1 != null && str1.contains("'")) {
                            insertSql.append("\"" + str1 + "\",");
                        } else {
                            insertSql.append(resultMmdStation.getString(i) == null ? "null," : "'" + resultMmdStation.getString(i) + "',");
                        }
                        continue;
                    }
                    insertSql.append(resultMmdStation.getString(i) == null ? "null," : "'" + resultMmdStation.getString(i) + "',");
                }
                //删除最后一个“ ‘ ”
                insertSql.delete(insertSql.length() - 1, insertSql.length());
                insertSql.append(")");
                System.out.println(insertSql);
                stmtTestResult.execute(insertSql.toString());
                insertSql.delete(0, insertSql.length());
                //todo
                // 导出到onetomaney_stationplat_mmd 新建主键id替换原有主键
                //转到第一行记录  注意first 和 beforefirst区别
                resultPmcisStation.beforeFirst();
                while (resultPmcisStation.next()) {
                    insertSql.append("insert into onetomaney_stationplat_mmd values(null,'pmcis',");
                    for (int i = 1; i < 89; i++) {
                        //偶尔会出现 a'b这样的数据
                        if (i == 12) {
                            String str1 = resultPmcisStation.getString(i);
                            if (str1 != null && str1.contains("'")) {
                                insertSql.append("\"" + str1 + "\",");
                            } else {
                                insertSql.append(resultPmcisStation.getString(i) == null ? "null," : "'" + resultPmcisStation.getString(i) + "',");
                            }
                            continue;
                        }
                        //MMD多出来的字段
                        //todo
                        if (i == 65 || i == 72) {
                            insertSql.append("null,");
                            insertSql.append(resultPmcisStation.getString(i) == null ? "null," : "'" + resultPmcisStation.getString(i) + "',");
                            // System.out.println(sqlInsert);
                            continue;
                        }
                        insertSql.append(resultPmcisStation.getString(i) == null ? "null," : "'" + resultPmcisStation.getString(i) + "',");
                    }
                    //MMD多出来的字段
                    for (int i = 91; i < 97; i++) {
                        insertSql.append("null,");
                    }
                    //删除最后一个“ ‘ ”
                    insertSql.delete(insertSql.length() - 1, insertSql.length());
                    insertSql.append(")");
                    System.out.println(insertSql);
                    stmtTestResult.execute(insertSql.toString());
                    insertSql.delete(0, insertSql.length());
                }
            }
            //        如果返回一个记录，表示只有一个记录，进行逐字段比较。比较字段如下：台站号，站名，子站号，省市县代码，国家代码，行政区划 C_ADMINISTRATIVEDC
            else if (row == 1) {
                System.out.println("记录等于1");
                String mmd_indexsubnbr = resultMmdStation.getString("c_indexsubnbr") != null ? resultMmdStation.getString("c_indexsubnbr") : "";
                String mmd_stationnc = resultMmdStation.getString("c_stationnc") != null ? resultMmdStation.getString("c_stationnc") : "";
                String mmd_stationne = resultMmdStation.getString("c_stationne") != null ? resultMmdStation.getString("c_stationne") : "";
                String mmd_country = resultMmdStation.getString("c_country") != null ? resultMmdStation.getString("c_country") : "";
                String mmd_city = resultMmdStation.getString("c_city") != null ? resultMmdStation.getString("c_city") : "";
                String mmd_county = resultMmdStation.getString("c_county") != null ? resultMmdStation.getString("c_county") : "";
                String mmd_administrativedc = resultMmdStation.getString("c_administrativedc") != null ? resultMmdStation.getString("c_administrativedc") : "";

                String pmcis_indexsubnbr_ = resultPmcisStation.getString("c_indexsubnbr") != null ? resultPmcisStation.getString("c_indexsubnbr") : "";
                String pmcis_stationnc = resultPmcisStation.getString("c_stationnc") != null ? resultPmcisStation.getString("c_stationnc") : "";
                String pmcis_stationne = resultPmcisStation.getString("c_stationne") != null ? resultPmcisStation.getString("c_stationne") : "";
                String pmcis_country = resultPmcisStation.getString("c_country") != null ? resultPmcisStation.getString("c_country") : "";
                String pmcis_city = resultPmcisStation.getString("c_city") != null ? resultPmcisStation.getString("c_city") : "";
                String pmcis_county = resultPmcisStation.getString("c_county") != null ? resultPmcisStation.getString("c_county") : "";
                String pmcis_administrativedc = resultPmcisStation.getString("c_administrativedc") != null ? resultMmdStation.getString("c_administrativedc") : "";
                //TODO
                //如果全部相同，则表示完全一样 以same_stationplat_mmd为准导出 同样增加主键id替换原有主键
                if (mmd_indexsubnbr.equals(pmcis_indexsubnbr_) &&
                        mmd_stationnc.equals(pmcis_stationnc) &&
                        mmd_stationne.equals(pmcis_stationne) &&
                        mmd_country.equals(pmcis_country) &&
                        mmd_city.equals(pmcis_city) &&
                        mmd_county.equals(pmcis_county) &&
                        mmd_administrativedc.equals(pmcis_administrativedc)) {
                    System.out.println("主要字段一致");
                    insertSql.append("insert into onetoone_stationplat_mmd_pmcis values(null,'true',");
                }
                //不完全一致
                else {
                    //找出不一致的字段并加到false后面
                    System.out.println("主要字段不完全一致");
                    //todo 在false后添加不相同的字段名称
                    //获取不相同的字段名
                    StringBuilder temp = new StringBuilder();
                    if (!mmd_indexsubnbr.equals(pmcis_indexsubnbr_))
                        temp.append("c_indexsubnbr,");
                    if (!mmd_stationnc.equals(pmcis_stationnc))
                        temp.append("c_stationnc,");
                    if (!mmd_stationne.equals(pmcis_stationne))
                        temp.append("c_stationne,");
                    if (!mmd_country.equals(pmcis_country))
                        temp.append("c_country,");
                    if (!mmd_city.equals(pmcis_city))
                        temp.append("c_city,");
                    if (!mmd_county.equals(pmcis_county))
                        temp.append("c_county,");
                    if (!mmd_administrativedc.equals(pmcis_administrativedc))
                        temp.append("c_administrativedc,");
                    temp.delete(temp.length() - 1, temp.length());
                    insertSql.append("insert into onetoone_stationplat_mmd_pmcis values(null,'false," + temp + "',");
                }
                for (int i = 1, k = 1; i < 91; i++) {
                    //偶尔会出现 aa'b 这样的数据
                    if (i == 12) {
                        String str1 = resultMmdStation.getString(i);
                        String str2 = resultPmcisStation.getString(k);
                        if (str1 != null && str1.contains("'")) {
                            insertSql.append("\"" + str1 + "\",");
                        } else {
                            insertSql.append(resultMmdStation.getString(i) == null ? "null," : "'" + resultMmdStation.getString(i) + "',");
                        }
                        if (str2 != null && str2.contains("'")) {
                            insertSql.append("\"" + str2 + "\",");
                        } else {
                            insertSql.append(resultPmcisStation.getString(k) == null ? "null," : "'" + resultPmcisStation.getString(k) + "',");
                        }
                        k++;
                        continue;
                    }
                    //MMD多出来的字段
                    if (i == 65 || i == 73) {
                        insertSql.append(resultMmdStation.getString(i) == null ? "null," : "'" + resultMmdStation.getString(i) + "',");
                        insertSql.append("null,");
                        continue;
                    }
                    //时间戳
                    if (i == 74 || i == 75) {
                        insertSql.append(resultMmdStation.getString(i) == null ? "null," : "'" + resultMmdStation.getTimestamp(i) + "',");
                        insertSql.append(resultPmcisStation.getString(k) == null ? "null," : "'" + resultPmcisStation.getTimestamp(k) + "',");
                        k++;
                        continue;
                    }
                    insertSql.append(resultMmdStation.getString(i) == null ? "null," : "'" + resultMmdStation.getString(i) + "',");
                    insertSql.append(resultPmcisStation.getString(k) == null ? "null," : "'" + resultPmcisStation.getString(k) + "',");
                    k++;
                }
                //MMD多出来的字段
                for (int i = 91; i < 97; i++) {
                    insertSql.append(resultMmdStation.getString(i) == null ? "null," : "'" + resultMmdStation.getString(i) + "',");
                    insertSql.append("null,");
                }
                //删除最后一个“ ‘ ”
                insertSql.delete(insertSql.length() - 1, insertSql.length());
                insertSql.append(")");
                System.out.println(insertSql);
                stmtTestResult.execute(insertSql.toString());
                insertSql.delete(0, insertSql.length());
            } else {
                //TODO 如果没有记录呢？？？？ 表示多出来的数据
                //导出到morethanpmcis_stationplat_mmd
                insertSql.append("insert into morethanpmcis_stationplat_mmd values(null,");
                for (int i = 1; i < 97; i++) {
                    if (i == 12) {
                        String str1 = resultMmdStation.getString(i);
                        if (str1 != null && str1.contains("'")) {
                            insertSql.append("\"" + str1 + "\",");
                        } else {
                            insertSql.append(resultMmdStation.getString(i) == null ? "null," : "'" + resultMmdStation.getString(i) + "',");
                        }
                        continue;
                    }
                    if (resultMmdStation.getString(i) == null) {
                        insertSql.append("null,");
                        continue;
                    }
                    insertSql.append("'" + resultMmdStation.getString(i) + "',");
                }
                //删除最后一个“ ‘ ”
                insertSql.delete(insertSql.length() - 1, insertSql.length());
                insertSql.append(")");
                stmtTestResult.execute(insertSql.toString());
                insertSql.delete(0, insertSql.length());
                System.out.println("台站关系不一致");
                System.out.println("没有记录");
            }
        }
    }

    /**
     *     查出MMD库的数据 再查询pmcis的数据进行比较
     */
    //pmcis 比 mmd多的
    public static void stationplatPmcisMain() throws SQLException {
        Connection connectionMmd = XGMmdStandar.getConnection();
        Connection connectionPmcis = JDBCUtilsPmcis.getConnection();
        Connection connectionTestResult = JDBCUtilsTestResult.getConnection();
        stmtMdm = connectionMmd.createStatement();
        stmtPmcis = connectionPmcis.createStatement();
        stmtTestResult = connectionTestResult.createStatement();
        ResultSet resultMmdStation;
        //查找台站数据  先限制返回条数不然会不断创建新的resultset对象导致堆溢出
        ResultSet resultPmcisStationp = stmtPmcis.executeQuery("SELECT * FROM tab_omin_cm_cc_stationplat limit 30000 offset 120000 ");
        insertSql = new StringBuilder("");
        int j = 0;
        while (resultPmcisStationp.next()) {
            System.out.println(++j);
            insertSql.append("insert into morethanmmd_stationplat_pmcis values(null,");
            //获取每条记录的区站号
            String c_indexnbr = resultPmcisStationp.getString("c_indexnbr");
            resultMmdStation = stmtMdm.executeQuery("SELECT * FROM tab_omin_cm_cc_stationplat where " +
                    "C_INDEXNBR ='" + c_indexnbr + "'");
            //转到最后一行记录
            resultMmdStation.last();
            int row = resultMmdStation.getRow();
            //pmcis中存在  mmd不存在 导入到 morethanmmd_stationplat_pmcis
            if (row == 0) {
                System.out.println(insertSql);
                System.out.println("pmcis比mmd多的数据");
                for (int i = 1; i < 89; i++) {
                    if (i == 12) {
                        String str1 = resultPmcisStationp.getString(i);
                        if (str1 != null && str1.contains("'")) {
                            insertSql.append("\"" + str1 + "\",");
                        } else {
                            insertSql.append(resultPmcisStationp.getString(i) == null ? "null," : "'" + resultPmcisStationp.getString(i) + "',");
                        }
                        continue;
                    }
                    insertSql.append(resultPmcisStationp.getString(i) == null ? "null," : "'" + resultPmcisStationp.getString(i) + "',");
                }
                //删除最后一个“ ‘ ”
                insertSql.delete(insertSql.length() - 1, insertSql.length());
                insertSql.append(")");
                System.out.println(insertSql);
                stmtTestResult.execute(insertSql.toString());
                System.out.println("没有记录");
            }
            if (j == 20000)
                System.gc();
            insertSql.delete(0, insertSql.length());
        }
    }
}
