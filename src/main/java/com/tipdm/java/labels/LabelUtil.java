package com.tipdm.java.labels;

import com.tipdm.java.utils.Utils;

import java.io.IOException;

/**
 * Created by ch on 2018/9/11
 */
public class LabelUtil {
    public static String getLabel(String label) throws IOException {
        String sql =null;
        String condition = Utils.getSQL(label);
        if(label.equals("消费内容")){
             sql ="select distinct phone_no,case "+condition+" from mmconsume_billevent_process";
        }else if(label.equals("电视消费水平") || label.equals("宽带消费水平")){
            sql ="select t2.phone_no,case "+condition+" from\n" +
                    "(select t1.phone_no,sum(real_pay)/3 as fee_per_month  from (select phone_no,nvl(should_pay,0)-nvl(favour_fee,0) as real_pay from mmconsume_billevent_process) t1 group by t1.phone_no) t2";
        }else if(label.equals("宽带产品带宽")){
            sql = "select b.phone_no, case "+condition+" from(select a.phone_no,a.optdate,a.prodname,a.sm_name,row_number() over (partition by a.phone_no order by a.optdate desc) rank from (select phone_no,prodname ,expdate,optdate,sm_name from order_index_process where effdate < from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') and from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') < expdate) a) b where b.rank=1 and b.sm_name='珠江宽频'";
        }else  if ("销售品名称".equals(label)){
            sql ="select phone_no,case "+condition+" from(\n" +
                    "select phone_no,offername from \n" +
                    "(select t2.phone_no,t2.optdate,t2.offername,row_number() over (partition by t2.phone_no order by t2.optdate desc) rank from\n" +
                    "(select t1.phone_no,t1.offername,t1.optdate from \n" +
                    "(select * from order_index_process where cost>0 and offername not like '%空包%') t1 where t1.sm_name like '%电视%' and t1.mode_time='Y' and t1.offertype=0 and t1.prodstatus='YY' and t1.effdate < from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') and from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') < t1.expdate) t2) t3 where t3.rank=1 union all select  phone_no,offername from \n" +
                    "(select * from order_index_process where cost>0 and offername not like '%空包%') t3 where t3.sm_name like '%电视%' and t3.mode_time='Y' and t3.offertype=1 and t3.prodstatus='YY' and t3.effdate < from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') and from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') < t3.expdate\n" +
                    "union all select phone_no,offername from \n" +
                    "(select t2.phone_no,t2.optdate,t2.offername,row_number() over (partition by t2.phone_no order by t2.optdate desc) rank from\n" +
                    "(select t1.phone_no,t1.offername,t1.optdate from \n" +
                    "(select * from order_index_process where cost>0 and offername not like '%空包%') t1 where t1.sm_name like '%珠江宽频%' and t1.effdate < from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') and from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') < t1.expdate) t2 )t3 where t3.rank=1) tt";
        }else if("业务品牌".equals(label)){
            sql="select phone_no,case "+condition+" from mediamatch_usermsg_process where sm_name not like '%模拟有线电视%' or sm_name not like '%番通%'";
        }else if("电视入网程度".equals(label)){
            sql = "select t1.phone_no,case "+ condition+" from(\n" +
                    "select phone_no,max(datediff(current_date(),open_time)/365) as T from mediamatch_usermsg_process where sm_name like '%电视%' and open_time is not NULL group by phone_no) t1";
        }else if("宽带入网程度".equals(label)){
           sql = "select t1.phone_no,case "+condition+" from\n" +
                   "(select phone_no,max(datediff(current_date(),open_time)/365) as T from mediamatch_usermsg_process where sm_name='珠江宽频' and force like '%宽带生效%' and sm_code='b0' group by phone_no) t1";
        }else {
            System.out.printf("标签名称没有找到");
            System.exit(1);
        }
        return sql;
    }
}
