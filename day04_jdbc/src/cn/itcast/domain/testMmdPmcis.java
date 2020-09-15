package cn.itcast.domain;

import cn.itcast.util.JDBCUtilsPmcis;
import cn.itcast.util.JDBCUtilsTestResult;
import cn.itcast.util.XGJDBCUtilsMdm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;

import static java.lang.System.exit;

public class testMmdPmcis {
    static Statement stmtMdm = null;
    static Statement stmtPmcis = null;
    static Statement stmtTestResult = null;
    static StringBuilder insertSql;

    public static void main(String[] args) {
        try {
          //  combinationArstationnetship();
          //  testStationnetshipMmdMain();
            //  stationnetshipPmcisMain();
            //test();
            //  pmcisMore();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 组合mmd和pmcis的酸雨、台站关系表辅助表
     */
    public static void combinationArstationnetship() throws SQLException {
        //虚谷
        Connection connectionMmd = XGJDBCUtilsMdm.getConnection();
        // pmcis
        Connection connectionPmcis = JDBCUtilsPmcis.getConnection();
        //
        // resulttest
        Connection connectionTestResult = JDBCUtilsTestResult.getConnection();
        String selectSql = "SELECT a.C_INDEXNBR, b.C_SNET_ID FROM tab_omin_cm_cc_stationplat a JOIN " +
                "tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID " +
                "JOIN tab_omin_cm_cc_arstationnetship c  on  b.C_SNETSHIP_ID = c.C_SNETSHIP_ID  limit 500";
        Statement mmdStatement = connectionMmd.createStatement();
        Statement pmcisStatement = connectionPmcis.createStatement();
        Statement testResultStatementStatement = connectionTestResult.createStatement();


        ResultSet resultSetMmd = mmdStatement.executeQuery(selectSql);
        while(resultSetMmd.next()){
            String c_indexnbr = resultSetMmd.getString(1);
            String c_snet_id = resultSetMmd.getString(2);

            String selectSqlArstation = "SELECT a.C_INDEXNBR, b.C_SNET_ID FROM tab_omin_cm_cc_stationplat a JOIN " +
                    "tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID JOIN " +
                    "tab_omin_cm_cc_arstationnetship c ON b.C_SNETSHIP_ID = c.C_SNETSHIP_ID WHERE" +
                    " a.c_indexnbr = '"+c_indexnbr+"' AND c_snet_id = '"+c_indexnbr+"'";



        }


        //ResultSet resultSetPmcis = pmcisStatement.executeQuery(selectSql);
       // StringBuilder insertSql = new StringBuilder("insert into combination_arstationnetship values(null,");


    }

    /**
     * 比较Stationnetship 以mmd的台站号为准
     *
     * @throws SQLException
     */
    public static void testStationnetshipMmdMain() throws SQLException {
        //虚谷
        Connection connectionMmd = XGJDBCUtilsMdm.getConnection();
        //博华 pmcis
        Connection connectionPmcis = JDBCUtilsPmcis.getConnection();
        //博华 resulttest
        Connection connectionTestResult = JDBCUtilsTestResult.getConnection();

        stmtMdm = connectionMmd.createStatement();
        stmtPmcis = connectionPmcis.createStatement();
        stmtTestResult = connectionTestResult.createStatement();
        insertSql = new StringBuilder("");
        //        SELECT
        //                GROUP_CONCAT(C_SNET_ID) as mmdSnet
        //        FROM
        //        tab_omin_cm_cc_stationplat a
        //        JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID
        //        where a.C_INDEXNBR = '05087'
        //        GROUP BY C_INDEXNBR
        //
        //

        //                寻找PMCIS站网的数据
        //        SELECT
        //                -- 			b.*
        //                GROUP_CONCAT(C_SNET_ID) as pmcisSnet
        //        FROM
        //        tab_omin_cm_cc_stationplat a
        //        JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID
        //        where a.C_INDEXNBR = '05087'
        //        GROUP BY C_INDEXNBR
        //// 比较站网关系。站网数据比较，是否在同一个站网中，即 C_SNET_ID 相同
        //                寻找站网的数据
        //查询mmd表的C_SNET_ID
        ResultSet resultMmdStation = stmtMdm.executeQuery("SELECT * FROM tab_omin_cm_cc_stationplat limit 1000  offset  0 ");
        //
        int j = 0;
        while (resultMmdStation.next()) {
            System.out.println(++j);
            //获取每条记录的台站号
            String c_indexnbr = resultMmdStation.getString("c_indexnbr   ");
            //一个statement对象 对应一个resultset对象
            Statement stmtmmd1 = connectionMmd.createStatement();
            Statement stmtPmcis1 = connectionPmcis.createStatement();
            String MmdSnetQuerySql = "SELECT wm_concat(C_SNET_ID) AS mmdSnet  FROM tab_omin_cm_cc_stationplat " +
                    "a JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID WHERE a.C_INDEXNBR = '" + c_indexnbr + "' GROUP BY a.C_INDEXNBR";
            //根据台站号和站网号去查询站网表中的记录 并保存到组合表中
            //SELECT b.* FROM tab_omin_cm_cc_stationplat a JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID WHERE a.C_INDEXNBR = '' AND b.C_SNET_ID = ''
            //得到执行结果
            System.out.println(MmdSnetQuerySql);
            exit(0);
            ResultSet resultMmdSnetQuery = stmtmmd1.executeQuery(MmdSnetQuerySql);
            //  System.out.println(resultMmdSnetQuery.getString(1));
            //查询pmcis表的C_SNET_ID
            String pmcisSnetQuerySql = "SELECT GROUP_CONCAT(C_SNET_ID) AS pmcisSnet  FROM tab_omin_cm_cc_stationplat " +
                    "a JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID WHERE a.C_INDEXNBR = '" + c_indexnbr + "' GROUP BY a.C_INDEXNBR";
            //得到执行结果
            ResultSet resultpmcisSnetQuery = stmtPmcis1.executeQuery(pmcisSnetQuerySql);
            //查询站网表记录
            Statement statementMmdNetship = connectionMmd.createStatement();
            Statement statementPmcisNetship = connectionPmcis.createStatement();
            ResultSet resultSetMmd = null;
            ResultSet resultSetPmcis = null;
            stmtTestResult = connectionTestResult.createStatement();
            HashSet<String> mmdSnet;
            HashSet<String> pmcisSnet;
            if (resultMmdSnetQuery.next()) {
                mmdSnet = new HashSet<>(Arrays.asList(resultMmdSnetQuery.getString(1).split(",")));
            } else {
                mmdSnet = new HashSet<>();
            }
            if (resultpmcisSnetQuery.next()) {
                pmcisSnet = new HashSet<>(Arrays.asList(resultpmcisSnetQuery.getString(1).split(",")));
            } else {
                pmcisSnet = new HashSet<>();
            }

            //        对比两个字符串mmdSnet，pmcisSnet 。split + set比较
            StringBuilder insertSql = new StringBuilder();
            for (String snetidMmd : mmdSnet) {
                insertSql.append("insert into difference_stationnetship values(null,'" + c_indexnbr + "',");
                //p == m
                if (pmcisSnet.contains(snetidMmd)) {
                    insertSql.append("'" + snetidMmd + "'," + "'" + snetidMmd + "'," + "'p==m')");
                    // System.out.println(insertSql);
                    stmtTestResult.execute(insertSql.toString());
                    insertSql.delete(0, insertSql.length());
                    //得到自增id
                    ResultSet resultLastId = stmtTestResult.executeQuery("SELECT LAST_INSERT_ID()");
                    resultLastId.next();
                    int lastId = resultLastId.getInt(1);
                    System.out.println(lastId);
                    //todo
                    //向两个数据库的站网表中 查出对应的两条记录并插入到组合表中
                    String selectSql = "SELECT b.* FROM tab_omin_cm_cc_stationplat a JOIN " +
                            "tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID WHERE a.C_INDEXNBR = '" + c_indexnbr + "' AND b.C_SNET_ID = '" + snetidMmd + "'";
                    //System.out.println(selectSql);
                    resultSetMmd = statementMmdNetship.executeQuery(selectSql);
                    resultSetPmcis = statementPmcisNetship.executeQuery(selectSql);
                    //结果集必不为空
                    resultSetMmd.next();
                    insertSql.append("insert into stationnetship_mmd_pmcis values(null,'mmd'," + lastId + ",");
                    for (int i = 1; i < 11; i++) {
                        insertSql.append(resultSetMmd.getString(i) == null ? "null," : "'" + resultSetMmd.getString(i) + "',");
                    }
                    insertSql.append(resultSetMmd.getString("C_INSTR_MODEL") == null ? "null," : "'" + resultSetMmd.getString("C_INSTR_MODEL") + "',");
                    insertSql.append(resultSetMmd.getString("C_MF") == null ? "null," : "'" + resultSetMmd.getString("C_MF") + "',");
                    insertSql.append("null,null,null,null,null,");
                    insertSql.append(resultSetMmd.getString("VERSION") == null ? "null," : "'" + resultSetMmd.getString("VERSION") + "',");
                    insertSql.append("null,null,null,null,null,");
                    insertSql.append(resultSetMmd.getString("C_STARTTIME   ") == null ? "null," : "'" + resultSetMmd.getString("C_STARTTIME") + "',");
                    insertSql.append(resultSetMmd.getString("C_ENDTIME     ") == null ? "null," : "'" + resultSetMmd.getString("C_ENDTIME") + "',");
                    insertSql.append(resultSetMmd.getString("C_TIMESYSTEM  ") == null ? "null," : "'" + resultSetMmd.getString("C_TIMESYSTEM") + "',");
                    insertSql.append(resultSetMmd.getString("C_EXCHANGECODE") == null ? "null," : "'" + resultSetMmd.getString("C_EXCHANGECODE") + "',");
                    insertSql.append(resultSetMmd.getString("C_OBSVMODE    ") == null ? "null," : "'" + resultSetMmd.getString("C_OBSVMODE") + "',");
                    insertSql.append(resultSetMmd.getString("C_OBSVCOUNT   ") == null ? "null," : "'" + resultSetMmd.getString("C_OBSVCOUNT") + "',");
                    insertSql.append(resultSetMmd.getString("C_OBSVTIMES   ") == null ? "null," : "'" + resultSetMmd.getString("C_OBSVTIMES") + "',");
                    insertSql.append(resultSetMmd.getString("EQ_MODEL") == null ? "null," : "'" + resultSetMmd.getString("EQ_MODEL") + "',");
                    insertSql.append(resultSetMmd.getString("MANUFACTURE") == null ? "null)" : "'" + resultSetMmd.getString("MANUFACTURE") + "')");
                    stmtTestResult.execute(insertSql.toString());
                    System.out.println(insertSql);
                    insertSql.delete(0, insertSql.length());

                    //pmcis：站网表记录
                    resultSetPmcis.next();
                    insertSql.append("insert into stationnetship_mmd_pmcis values(null,'pmcis'," + lastId + ",");
                    for (int i = 1; i < 31; i++) {
                        insertSql.append(resultSetPmcis.getString(i) == null ? "null," : "'" + resultSetPmcis.getString(i) + "',");
                    }
                    insertSql.append("null,null)");
                    stmtTestResult.execute(insertSql.toString());
                    System.out.println(insertSql);
                    insertSql.delete(0, insertSql.length());
                }//m > p 直在组合表中插入 MMD的站网表记录
                else {
                    insertSql.append("'" + snetidMmd + "'," + "''," + "'m>p')");
                    System.out.println(insertSql);
                    stmtTestResult.execute(insertSql.toString());
                    insertSql.delete(0, insertSql.length());
                    //得到自增id
                    ResultSet resultLastId = stmtTestResult.executeQuery("SELECT LAST_INSERT_ID()");
                    resultLastId.next();
                    String lastId = resultLastId.getString(1);
                    //  System.out.println(lastId);
                    //todo
                    //在MMD数据库的站网表中 查出对应记录并插入到组合表中
                    String selectSql = "SELECT b.* FROM tab_omin_cm_cc_stationplat a JOIN " +
                            "tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID WHERE a.C_INDEXNBR = '" + c_indexnbr + "' AND b.C_SNET_ID = '" + snetidMmd + "'";
                    //System.out.println(selectSql);
                    resultSetMmd = statementMmdNetship.executeQuery(selectSql);
                    resultSetMmd.next();
                    insertSql.append("insert into stationnetship_mmd_pmcis values(null,'mmd'," + lastId + ",");
                    for (int i = 1; i < 11; i++) {
                        insertSql.append(resultSetMmd.getString(i) == null ? "null," : "'" + resultSetMmd.getString(i) + "',");
                    }
                    insertSql.append(resultSetMmd.getString("C_INSTR_MODEL") == null ? "null," : "'" + resultSetMmd.getString("C_INSTR_MODEL") + "',");
                    insertSql.append(resultSetMmd.getString("C_MF") == null ? "null," : "'" + resultSetMmd.getString("C_MF") + "',");
                    insertSql.append("null,null,null,null,null,");
                    insertSql.append(resultSetMmd.getString("VERSION") == null ? "null," : "'" + resultSetMmd.getString("VERSION") + "',");
                    insertSql.append("null,null,null,null,null,");
                    insertSql.append(resultSetMmd.getString("C_STARTTIME   ") == null ? "null," : "'" + resultSetMmd.getString("C_STARTTIME") + "',");
                    insertSql.append(resultSetMmd.getString("C_ENDTIME     ") == null ? "null," : "'" + resultSetMmd.getString("C_ENDTIME") + "',");
                    insertSql.append(resultSetMmd.getString("C_TIMESYSTEM  ") == null ? "null," : "'" + resultSetMmd.getString("C_TIMESYSTEM") + "',");
                    insertSql.append(resultSetMmd.getString("C_EXCHANGECODE") == null ? "null," : "'" + resultSetMmd.getString("C_EXCHANGECODE") + "',");
                    insertSql.append(resultSetMmd.getString("C_OBSVMODE    ") == null ? "null," : "'" + resultSetMmd.getString("C_OBSVMODE") + "',");
                    insertSql.append(resultSetMmd.getString("C_OBSVCOUNT   ") == null ? "null," : "'" + resultSetMmd.getString("C_OBSVCOUNT") + "',");
                    insertSql.append(resultSetMmd.getString("C_OBSVTIMES   ") == null ? "null," : "'" + resultSetMmd.getString("C_OBSVTIMES") + "',");
                    insertSql.append(resultSetMmd.getString("EQ_MODEL") == null ? "null," : "'" + resultSetMmd.getString("EQ_MODEL") + "',");
                    insertSql.append(resultSetMmd.getString("MANUFACTURE") == null ? "null)" : "'" + resultSetMmd.getString("MANUFACTURE") + "')");
                    stmtTestResult.execute(insertSql.toString());
                    System.out.println(insertSql);
                    insertSql.delete(0, insertSql.length());
                /*     resultSetPmcis = statementPmcisNetship.executeQuery(selectSql);
                    //结果集必为空
                   if(resultSetPmcis.next()){
                       System.out.println("m>p时 pmcis不为空");
                       exit(0);
                   }*/
                }
            }
            // p > m
            for (String snetidPmcis : pmcisSnet) {
                if (!mmdSnet.contains(snetidPmcis)) {
                    insertSql.append("insert into difference_stationnetship values(null,'" + c_indexnbr + "',");
                    insertSql.append("''," + "'" + snetidPmcis + "'," + "'p>m')");
                    // System.out.println(insertSql);
                    stmtTestResult.execute(insertSql.toString());
                    insertSql.delete(0, insertSql.length());
                    //得到自增id
                    ResultSet resultLastId = stmtTestResult.executeQuery("SELECT LAST_INSERT_ID()");
                    resultLastId.next();
                    int lastId = resultLastId.getInt(1);
                    //System.out.println(lastId);
                    //todo
                    //向两个数据库的站网表中 查出对应的两条记录并插入到组合表中
                    String selectSql = "SELECT b.* FROM tab_omin_cm_cc_stationplat a JOIN " +
                            "tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID WHERE a.C_INDEXNBR = '" + c_indexnbr + "' AND b.C_SNET_ID = '" + snetidPmcis + "'";
                    //System.out.println(selectSql);
                    resultSetMmd = statementMmdNetship.executeQuery(selectSql);
                    resultSetPmcis = statementPmcisNetship.executeQuery(selectSql);
                    //结果集必为空
                    resultSetMmd.next();
                    //pmcis：站网表记录 不为空
                    resultSetPmcis.next();
                    insertSql.append("insert into stationnetship_mmd_pmcis values(null,'pmcis'," + lastId + ",");
                    for (int i = 1; i < 31; i++) {
                        insertSql.append(resultSetPmcis.getString(i) == null ? "null," : "'" + resultSetPmcis.getString(i) + "',");
                    }
                    insertSql.append("null,null)");
                    stmtTestResult.execute(insertSql.toString());
                    System.out.println(insertSql);
                    insertSql.delete(0, insertSql.length());
                }
            }
        }
    }


    /**
     * 以pmcis的台站号为准比较stationnetship 只找pmcis有 mmd没有的数据
     *
     * @throws SQLException
     */
    public static void stationnetshipPmcisMain() throws SQLException {
        Connection connectionMmd = XGJDBCUtilsMdm.getConnection();
        Connection connectionPmcis = JDBCUtilsPmcis.getConnection();
        Connection connectionTestResult = JDBCUtilsTestResult.getConnection();
        stmtMdm = connectionMmd.createStatement();
        stmtPmcis = connectionPmcis.createStatement();
        stmtTestResult = connectionTestResult.createStatement();
        ResultSet resultPmcis = stmtPmcis.executeQuery("SELECT * FROM tab_omin_cm_cc_stationplat limit 10000  offset  0 ");
        insertSql = new StringBuilder();
        //
        int j = 0;
        while (resultPmcis.next()) {
            System.out.println(++j);
            //获取每条记录的台站号
            String c_indexnbr = resultPmcis.getString("c_indexnbr");
            //一个statement对象 对应一个resultset对象
            Statement stmtmmd1 = connectionMmd.createStatement();
            Statement stmtPmcis1 = connectionPmcis.createStatement();
            String MmdSnetQuerySql = "SELECT wm_concat(C_SNET_ID) AS mmdSnet  FROM tab_omin_cm_cc_stationplat " +
                    "a JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID WHERE a.C_INDEXNBR = '" + c_indexnbr + "' GROUP BY a.C_INDEXNBR";
            //System.out.println(MmdSnetQuerySql);
            //根据台站号和站网号去查询站网表中的记录 并保存到组合表中
            //得到执行结果
            ResultSet resultMmdSnetQuery = stmtmmd1.executeQuery(MmdSnetQuerySql);

            //查询pmcis表的C_SNET_ID
            String pmcisSnetQuerySql = "SELECT GROUP_CONCAT(C_SNET_ID) AS pmcisSnet  FROM tab_omin_cm_cc_stationplat " +
                    "a JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID WHERE a.C_INDEXNBR = '" + c_indexnbr + "' GROUP BY a.C_INDEXNBR";
            //得到执行结果
            //   System.out.println(pmcisSnetQuerySql);
            ResultSet resultpmcisSnetQuery = stmtPmcis1.executeQuery(pmcisSnetQuerySql);

            Statement statementMmdNetship = connectionMmd.createStatement();
            Statement statementPmcisNetship = connectionPmcis.createStatement();
            //        对比两个字符串mmdSnet，pmcisSnet 。split + set比较

            //pmcis中绑定了站网 但是mmd中该台站没有绑定站网 则记录该数据
            if (!resultMmdSnetQuery.next() && resultpmcisSnetQuery.next()) {
                HashSet<String> pmcisSnet = new HashSet<>(Arrays.asList(resultpmcisSnetQuery.getString("pmcisSnet").split(",")));
                for (String snetidPmcis : pmcisSnet) {
                    insertSql.append("insert into difference_stationnetship values(null,'" + c_indexnbr + "',");
                    insertSql.append("''," + "'" + snetidPmcis + "'," + "'p>m')");
                    // System.out.println(insertSql);
                    stmtTestResult.execute(insertSql.toString());
                    insertSql.delete(0, insertSql.length());
                    //得到自增id
                    ResultSet resultLastId = stmtTestResult.executeQuery("SELECT LAST_INSERT_ID()");
                    resultLastId.next();
                    int lastId = resultLastId.getInt(1);
                    //System.out.println(lastId);
                    //todo
                    //在pmcis数据库的站网表中 查出对应的记录并插入到组合表中
                    String selectSql = "SELECT b.* FROM tab_omin_cm_cc_stationplat a JOIN " +
                            "tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID WHERE a.C_INDEXNBR = '" + c_indexnbr + "' AND b.C_SNET_ID = '" + snetidPmcis + "'";
                    //System.out.println(selectSql);
                    ResultSet resultSetPmcis = statementPmcisNetship.executeQuery(selectSql);
                    //pmcis：站网表记录 不为空
                    resultSetPmcis.next();
                    insertSql.append("insert into stationnetship_mmd_pmcis values(null,'pmcis'," + lastId + ",");
                    for (int i = 1; i < 31; i++) {
                        insertSql.append(resultSetPmcis.getString(i) == null ? "null," : "'" + resultSetPmcis.getString(i) + "',");
                    }
                    insertSql.append("null,null)");
                    stmtTestResult.execute(insertSql.toString());
                    System.out.println(insertSql);
                    insertSql.delete(0, insertSql.length());
                    System.out.println("存在");
                }

            }
        }
    }


    /**
     * @throws Exception
     */
    public static void stationplatMmdMain() throws Exception {
        Connection connectionMmd = XGJDBCUtilsMdm.getConnection();
        Connection connectionPmcis = JDBCUtilsPmcis.getConnection();
        Connection connectionTestResult = JDBCUtilsTestResult.getConnection();
        stmtMdm = connectionMmd.createStatement();
        stmtPmcis = connectionPmcis.createStatement();
        stmtTestResult = connectionTestResult.createStatement();
        insertSql = new StringBuilder("");
        //查找台站数据  先限制返回条数不然测试时间太长
        ResultSet resultMmdStation = stmtMdm.executeQuery("SELECT * FROM tab_omin_cm_cc_stationplat limit 30000  offset  90001 ");
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

            //
            //TODO 比较资料台站关系,不用处理，直接使用pmcis的数据重新处理。？？？
            //

            //        SELECT
            //                GROUP_CONCAT(C_SNET_ID) as mmdSnet
            //        FROM
            //        tab_omin_cm_cc_stationplat a
            //        JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID
            //        where a.C_INDEXNBR = '05087'
            //        GROUP BY C_INDEXNBR
            //
            //

            //                寻找PMCIS站网的数据
            //        SELECT
            //                -- 			b.*
            //                GROUP_CONCAT(C_SNET_ID) as pmcisSnet
            //        FROM
            //        tab_omin_cm_cc_stationplat a
            //        JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID
            //        where a.C_INDEXNBR = '05087'
            //        GROUP BY C_INDEXNBR
            //// 比较站网关系。站网数据比较，是否在同一个站网中，即 C_SNET_ID 相同
            //                寻找站网的数据
            //查询mmd表的C_SNET_ID

            //一个statement对象 对应一个resultset对象
      /*      Statement stmtmmd1 = connectionMmd.createStatement();
            Statement stmtPmcis1 = connectionPmcis.createStatement();
            String MmdSnetQuerySql = "SELECT wm_concat(C_SNET_ID) AS mmdSnet  FROM tab_omin_cm_cc_stationplat " +
                    "a JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID WHERE a.C_INDEXNBR = '" + c_indexnbr + "' GROUP BY a.C_INDEXNBR";
            //得到执行结果
            System.out.println(MmdSnetQuerySql);
            ResultSet resultMmdSnetQuery = stmtmmd1.executeQuery(MmdSnetQuerySql);

            //查询pmcis表的C_SNET_ID
            String pmcisSnetQuerySql = "SELECT GROUP_CONCAT(C_SNET_ID) AS pmcisSnet  FROM tab_omin_cm_cc_stationplat " +
                    "a JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID WHERE a.C_INDEXNBR = '" + c_indexnbr + "' GROUP BY a.C_INDEXNBR";
            //得到执行结果
            ResultSet resultpmcisSnetQuery = stmtPmcis1.executeQuery(pmcisSnetQuerySql);

            //查询对应的stationnetship记录
            String Stationnewtsship = "SELECT b.*  FROM tab_omin_cm_cc_stationplat " +
                    "a JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID WHERE a.C_INDEXNBR = '"+c_indexnbr+"'";

            Statement statementMmdNetship = connectionMmd.createStatement();
            Statement statementPmcisNetship = connectionPmcis.createStatement();
            ResultSet resultSetMmdNetship = statementMmdNetship.executeQuery(Stationnewtsship);
            ResultSet resultSetPmcisNetship = statementPmcisNetship.executeQuery(Stationnewtsship);
            //        对比两个字符串mmdSnet，pmcisSnet 。split + set比较
            //String[] arrayResultmmdSnetQuery = resultmmdSnetQuery.getString("mmdSnet").split(",");
            // String[] arrayPmcisSnetQuerySql = resultpmcisSnetQuery.getString("pmcisSnet").split(",");
            String group_concat_csnetid_mmd;
            String group_concat_csnetid_pmcis;
            while (resultMmdSnetQuery.next() && resultpmcisSnetQuery.next()) {
                group_concat_csnetid_mmd = resultMmdSnetQuery.getString("mmdSnet");
                group_concat_csnetid_pmcis = resultpmcisSnetQuery.getString("pmcisSnet");
                //如果都相同，则对比站网表的所有字段 然后插入到组合表中
                //todo 比较字符串内容不能用equals考虑每个string
                if (group_concat_csnetid_mmd.equals(group_concat_csnetid_pmcis)) {
                    System.out.println(group_concat_csnetid_mmd);
                    System.out.println(group_concat_csnetid_pmcis);
                    System.out.println("如果mmdSnet比pmcis相同，则表示完全一样");
                    //标志是否全部字段都相等
                    int flag = 0;
                    for(int i=2; i<24; i++){
                        //如果有一个字段不相等
                        //todo 字段数不一样怎么进行字段的比较？
                        //把GROUP_CONCAT(C_SNET_ID)放入到组合表中

                    }
                    StringBuilder SqlInsertIntoMmd = new StringBuilder("insert into stationnetship_mmd values(");
                    for (int i = 2; i < 10; i++) {
                        SqlInsertIntoMmd.append("'" + resultMmdStation.getString(i) + "',");
                    }
                    //删除最后一个“ ‘ ”
                    SqlInsertIntoMmd.delete(SqlInsertIntoMmd.length() - 1, SqlInsertIntoMmd.length());
                    SqlInsertIntoMmd.append(")");
                    System.out.println(SqlInsertIntoMmd);
                    stmtTestResult.execute(SqlInsertIntoMmd.toString());
                }
                //        如果mmdSnet比pmcis多，则插入到MMD
                else if (group_concat_csnetid_mmd.length() > group_concat_csnetid_pmcis.length()) {
                    System.out.println(group_concat_csnetid_mmd);
                    System.out.println(group_concat_csnetid_pmcis);
                    System.out.println("如果mmdSnet比pmcis多，则导出mmd");
                    StringBuilder SqlInsertIntoMmd = new StringBuilder("insert into stationnetship_mmd values(");
                    for (int i = 2; i < 24; i++) {
                        SqlInsertIntoMmd.append("'" + resultMmdStation.getString(i) + "',");
                    }
                    //删除最后一个“ ‘ ”
                    SqlInsertIntoMmd.delete(SqlInsertIntoMmd.length() - 1, SqlInsertIntoMmd.length());
                    SqlInsertIntoMmd.append(")");
                    System.out.println(SqlInsertIntoMmd);
                    stmtTestResult.execute(SqlInsertIntoMmd.toString());
                }
                //        如果pmcis多，则导出 表结构以pmcis为准
                else if (group_concat_csnetid_mmd.length() < group_concat_csnetid_pmcis.length()) {
                    //todo
                    System.out.println(group_concat_csnetid_mmd);
                    System.out.println(group_concat_csnetid_pmcis);
                    System.out.println("如果pmcis比mmdSnet多，则导出pmcis");
                    StringBuilder SqlInsertIntoPmcis = new StringBuilder("insert into stationnetship_pmcis values(");
                    for (int i = 2; i < 32; i++) {
                        SqlInsertIntoPmcis.append("'" + resultpmcisSnetQuery.getString(i) + "',");
                    }
                    //删除最后一个“ ‘ ”
                    SqlInsertIntoPmcis.delete(SqlInsertIntoPmcis.length() - 1, SqlInsertIntoPmcis.length());
                    SqlInsertIntoPmcis.append(")");
                    System.out.println(SqlInsertIntoPmcis);
                    stmtTestResult.execute(SqlInsertIntoPmcis.toString());
                }
            }*/
        }


    }

    public static void stationplatPmcisMain() throws SQLException {
        Connection connectionMmd = XGJDBCUtilsMdm.getConnection();
        Connection connectionPmcis = JDBCUtilsPmcis.getConnection();
        Connection connectionTestResult = JDBCUtilsTestResult.getConnection();
        stmtMdm = connectionMmd.createStatement();
        stmtPmcis = connectionPmcis.createStatement();
        stmtTestResult = connectionTestResult.createStatement();
        ResultSet resultMmdStation;
        //查找台站数据  先限制返回条数不然测试时间太长
        ResultSet resultPmcisStationp = stmtPmcis.executeQuery("SELECT * FROM tab_omin_cm_cc_stationplat limit 30000 offset 128084 ");
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
            resultMmdStation = null;
            if (j == 20000)
                System.gc();
            insertSql.delete(0, insertSql.length());
        }
    }
}
