package com.atguigu.util;

import com.alibaba.fastjson.JSONObject;

public class ParseJsonData {

    //解析一个字符串，返回一个json对象
    public static JSONObject getJsonData(String data){
        try{
            return JSONObject.parseObject(data);
        }catch (Exception e){
            return null;
        }
    }
}
