package com.tipdm.java.utils;

import com.tipdm.java.SQLResolve;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by ch on 2018/9/10
 */
public class Utils {
    private static String path = Utils.class.getClassLoader().getResource("rules/consume_content.json").getPath();
    public static String getSQL(String label) throws IOException {
        File file = new File(path);
        String json = FileUtils.readFileToString(file);
        JSONObject jsonObject =JSONObject.fromObject(json);
        JSONArray jsonArray =jsonObject.getJSONArray(label);
        StringBuilder sql = new StringBuilder();
        if (jsonArray.size()>0){
            for(int i=0;i<jsonArray.size();i++){
                JSONObject obj =jsonArray.getJSONObject(i);
                String rules = (String)obj.get("rules");
                sql.append("when").append(" ").append(rules).append(" ");
                String la = (String)obj.get("label");
                sql.append("then ").append("\""+la+"\""+" ");
            }
            sql.append(" end as label,\""+label +"\" as parent_label");
        }else {
            System.out.println("rules/consume_content.json文件中没有此标签");
            System.exit(1);
        }
        return sql.toString();
    }
}
