package cn.itcast.domain;

import cn.itcast.util.JDBCUtilsMcm;
import cn.itcast.util.JDBCUtilsPmcis;
import cn.itcast.util.JDBCUtilsTestResult;

import java.sql.*;
import java.util.ArrayList;

public class Statinplat {
    public static void main(String[] args) throws SQLException {
        Statement stmtMcm=null;
        Statement stmtPmcis=null;
        ArrayList<String> list = new ArrayList<>();
        // Connection conn=null;
        //mcm.tab_omin_cm_cc_stationplat表 主表 的的所有主键
        ArrayList<String> list_C_SITEOPF_ID = new ArrayList<>();

        Connection connectionMcm = JDBCUtilsMcm.getConnection();

        Connection connectionPmcis = JDBCUtilsPmcis.getConnection();


        //查询主表 的所有主键
        String sql_stationplat = "select C_SITEOPF_ID,C_INDEXNBR from  tab_omin_cm_cc_stationplat limit 1200";

        //查询tab_omin_cm_cc_datum_station表的记录
        String sql_datum_station = "select * from  tab_omin_cm_cc_datum_station";

        //查询 tab_omin_cm_cc_stationnetship 表的C_SNETSHIP_ID
        String sql_stationnetship = "select C_SNETSHIP_ID from  tab_omin_cm_cc_stationnetship";

        //查询 tab_omin_cm_cc_obsqstationnetship 表的记录
        String sql_obsqstationnetship = "select * from  tab_omin_cm_cc_obsqstationnetship";

        //查询 tab_omin_cm_cc_arstationnetship 表的记录
        String relative1 = "select * from  tab_omin_cm_cc_arstationnetship";

        //查询 tab_omin_cm_cc_asmstationnetship 表的记录
        String relative2 = "select * from  tab_omin_cm_cc_asmstationnetship";

        //查询 tab_omin_cm_cc_awsstationnetship 表的记录
        String relative3 = "tab_omin_cm_cc_awsstationnetship";

        //查询 tab_omin_cm_cc_lpdstationnetship 表的记录
        String relative4 = "select * from  tab_omin_cm_cc_lpdstationnetship";

        //查询 tab_omin_cm_cc_radarstationnetship 表的记录
        String relative5 = "select * from  tab_omin_cm_cc_radarstationnetship";

        //查询 tab_omin_cm_cc_radistationnetship 表的记录
        String relative6 = "select * from  tab_omin_cm_cc_radistationnetship";

        //查询 tab_omin_cm_cc_uparstationnetship 表的记录
        String relative7 = "select * from  tab_omin_cm_cc_uparstationnetship";



        stmtMcm = connectionMcm.createStatement();
        stmtPmcis = connectionPmcis.createStatement();

        ResultSet resultSet = stmtMcm.executeQuery(sql_stationplat);
        while (resultSet.next()){
            //System.out.println(resultSet.getString(1));
            String str = resultSet.getString(2);

            ResultSet resultPmcisStation = stmtPmcis.executeQuery("select * from tab_omin_cm_cc_stationplat where C_INDEXNBR = '" + str + "'");
            //比较



        }

        System.out.println(list.size());
    }
}
