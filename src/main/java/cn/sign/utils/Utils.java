package cn.sign.utils;


import net.sf.json.JSONObject;

public class Utils {

    /**
     * 校验json字符串
     * @param jsonString
     * @return
     */
    public static Boolean validate(String jsonString) {
        if(null == jsonString) {
            return false;
        }
        try {
            JSONObject.fromObject(jsonString);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
