package org.opencb.opencga.catalog.utils;


import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.util.Base64;
import java.util.Date;

public class JwtUtils {


    public static Date getExpirationDate(String token) {
        Date res = new Date();
        if (StringUtils.isNotEmpty(token)) {
            String[] chunks = token.split("\\.");
            Base64.Decoder decoder = Base64.getUrlDecoder();
            String payload = new String(decoder.decode(chunks[1]));
            JSONParser parser = new JSONParser();
      /*  System.out.println(header);
        System.out.println(payload);
        System.out.println(StringUtils.join(chunks, ","));*/
            try {
                JSONObject json = (JSONObject) parser.parse(payload);
                Long exp = Long.parseLong(String.valueOf(json.get("exp")));
                res = new Date(exp * 1000);
            } catch (ParseException e) {
                return new Date();
            }
     /*   SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        System.out.println(sdf.format(res));*/
        }
        return res;
    }
}
