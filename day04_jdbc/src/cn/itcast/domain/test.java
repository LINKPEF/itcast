package cn.itcast.domain;

import cn.itcast.util.JDBCUtilsMcm;
import cn.itcast.util.JDBCUtilsPmcis;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class test {

    public static void main(String[] args) {

        try {
            test();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void test() throws Exception {
        //
        Statement stmtMcm = null;
        Statement stmtPmcis = null;
        ArrayList<String> list = new ArrayList<>();
        Connection connectionMcm = JDBCUtilsMcm.getConnection();
        Connection connectionPmcis = JDBCUtilsPmcis.getConnection();

        stmtMcm = connectionMcm.createStatement();
        stmtPmcis = connectionPmcis.createStatement();
        //查找台站数据
        ResultSet resultMcmStation = stmtMcm.executeQuery("SELECT * FROM tab_omin_cm_cc_stationplat a JOIN tab_omin_cm_cc_datum_station b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID");

        System.out.println("aa");
        while (resultMcmStation.next()) {
            //获取每条记录的台站号
            String c_indexnbr = resultMcmStation.getString("c_indexnbr");
            //        ////比较台站主表数据，台站号，站名，子站号，省市县代码，国家代码，行政区划C_ADMINISTRATIVEDC 。国家省市县名称字段统一update更改。
            //        查找PMCIS台站数据
            ResultSet resultPmcisStation = stmtPmcis.executeQuery("SELECT * FROM tab_omin_cm_cc_stationplat a JOIN tab_omin_cm_cc_datum_station b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID WHERE a.C_INDEXNBR = '" + c_indexnbr + "'");
            while (resultPmcisStation.next()) {
                //转到最后一行记录
                resultPmcisStation.last();
                int row = resultPmcisStation.getRow();
                if (row > 1) {
                    // 如果返回多个记录，表示台站号相同，但是子站号不同，或者国家代码不同，导出数据。
                    //todo
                } else if (row == 1) {
                    System.out.println(1);
                    //        如果返回一个记录，表示只有一个记录，进行逐字段比较。比较字段如下：台站号，站名，子站号，省市县代码，国家代码，行政区划 C_ADMINISTRATIVEDC
                    String mcm_indexsubnbr          = resultMcmStation.getString("c_indexsubnbr") != null ? resultMcmStation.getString("c_indexsubnbr") : "";
                    String mcm_stationnc                    = resultMcmStation.getString("c_stationnc") != null ? resultMcmStation.getString("c_stationnc") : "";
                    String mcm_stationne            = resultMcmStation.getString("c_stationne") != null ? resultMcmStation.getString("c_stationne") : "";
                    String mcm_country                       = resultMcmStation.getString("c_country") != null ? resultMcmStation.getString("c_country") : "";
                    String mcm_city                  = resultMcmStation.getString("c_city") != null ? resultMcmStation.getString("c_city") : "";
                    String mcm_county           = resultMcmStation.getString("c_county") != null ? resultMcmStation.getString("c_county") : "";
                    String mcm_administrativedc  = resultMcmStation.getString("c_administrativedc") != null ? resultMcmStation.getString("c_administrativedc") : "";

                    String pmcis_indexsubnbr_ = resultPmcisStation.getString("c_indexsubnbr") != null ? resultPmcisStation.getString("c_indexsubnbr") : "";
                    String pmcis_stationnc = resultPmcisStation.getString("c_stationnc") != null ? resultPmcisStation.getString("c_stationnc") : "";
                    String pmcis_stationne = resultPmcisStation.getString("c_stationne") != null ? resultPmcisStation.getString("c_stationne") : "";
                    String pmcis_country = resultPmcisStation.getString("c_country") != null ? resultPmcisStation.getString("c_country") : "";
                    String pmcis_city = resultPmcisStation.getString("c_city") != null ? resultPmcisStation.getString("c_city") : "";
                    String pmcis_county = resultPmcisStation.getString("c_county") != null ? resultPmcisStation.getString("c_county") : "";
                    String pmcis_administrativedc = resultPmcisStation.getString("c_administrativedc") != null ? resultMcmStation.getString("c_administrativedc") : "";

                    if (mcm_indexsubnbr.equals(pmcis_indexsubnbr_) &&
                            mcm_stationnc.equals(pmcis_stationnc) &&
                            mcm_stationne.equals(pmcis_stationne) &&
                            mcm_country.equals(pmcis_country) &&
                            mcm_city.equals(pmcis_city) &&
                            mcm_county.equals(pmcis_county) &&
                            mcm_administrativedc.equals(pmcis_administrativedc)) {
                        //        如果全部相同，则表示完全一样
                    } else {
                        //        如果有一个不同，则表示待处理，导出数据

                    }
                } else {
                    //TODO 如果没有记录呢？？？？
                }

            }


            //TODO 比较资料台站关系,不用处理，直接使用pmcis的数据重新处理。？？？

            //
            //


            //// 比较站网关系。站网数据比较，是否在同一个站网中，即 C_SNET_ID 相同
            //                寻找站网的数据
            //        SELECT
            //                -- 			b.*
            //                GROUP_CONCAT(C_SNET_ID) as mcmSnet
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
            //
            //        对比两个字符串mcmSnet，pmcisSnet 。split + set比较
            //        如果都相同，则表示完全一样
            //        如果mcmSnet比pmcis多，则忽略
            //        如果pmcis多，则导出
            //
            //
            //        ;;
            //        寻找有观测量的数据
            //                SELECT
            //        -- 	b.*,c.*
            //                a.C_INDEXNBR,b.C_SNET_ID,count(c.C_OBSQSN_ID)
            //        FROM
            //        tab_omin_cm_cc_stationplat a
            //        JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID
            //        JOIN tab_omin_cm_cc_obsqstationnetship c on b.C_SNETSHIP_ID = c.C_SNETSHIP_ID
            //        GROUP BY a.C_INDEXNBR,b.C_SNET_ID
            //        ORDER BY a.C_INDEXNBR
            //
            //                // 前提：观测量数据表同步，并同步update了观测量关系表数据外键值C_OBSQ_ID
            //                //// 比较观测量关系表。比较同一个站网下的观测量是否相同，即 C_OBSQ_ID 相同
            //                观测量
            //        SELECT
            //                -- 			b.*,c.*
            //                GROUP_CONCAT(C_OBSQ_ID) as mcmObsq
            //        FROM
            //        tab_omin_cm_cc_stationplat a
            //        JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID
            //        JOIN tab_omin_cm_cc_obsqstationnetship c on b.C_SNETSHIP_ID = c.C_SNETSHIP_ID
            //        where a.C_INDEXNBR = '05087' and C_SNET_ID = '0001'
            //        GROUP BY C_SNET_ID
            //
            //
            //                pmcis的观测量
            //        SELECT
            //                -- 			b.*,c.*
            //                GROUP_CONCAT(C_OBSQ_ID) as pmcisObsq
            //        FROM
            //        tab_omin_cm_cc_stationplat a
            //        JOIN tab_omin_cm_cc_stationnetship b ON a.C_SITEOPF_ID = b.C_SITEOPF_ID
            //        JOIN tab_omin_cm_cc_obsqstationnetship c on b.C_SNETSHIP_ID = c.C_SNETSHIP_ID
            //        where a.C_INDEXNBR = '05087' and C_SNET_ID = '0001'
            //        GROUP BY C_SNET_ID
            //
            //        对比两个字符串mcmSnet，pmcisSnet 。split + set比较
            //        如果都相同，则表示完全一样
            //        如果mcmSnet比pmcis多，则忽略
            //        如果pmcis多，则导出
            //
            //
            //
        }
    }
}
