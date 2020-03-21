package cn.sign.utils;

import java.util.HashMap;

public class ResultMap extends HashMap<String,Object>{

    public ResultMap(){
        put("code",200);
    }

    public static ResultMap success(){
        return new ResultMap();
    }

    public static ResultMap success(String msg){
        ResultMap result = new ResultMap();
        result.put("msg",msg);
        return result;
    }

    public static ResultMap success(String code, String msg){
        ResultMap result = new ResultMap();
        result.put("code",code);
        result.put("msg",msg);
        return result;
    }

    public static ResultMap error(){
        return error(500,"未知错误，请稍后重试!");
    }

    public static ResultMap error(String msg){
        return error(500,msg);
    }

    public static ResultMap error(int code, String msg){
        ResultMap result = new ResultMap();
        result.put("code",code);
        result.put("msg",msg);
        return result;
    }

    @Override
    public ResultMap put(String key, Object value){
        super.put(key,value);
        return this;
    }

}

