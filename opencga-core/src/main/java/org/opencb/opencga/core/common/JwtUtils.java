package org.opencb.opencga.core.common;


import io.jsonwebtoken.SignatureAlgorithm;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.models.JwtPayload;

import javax.crypto.spec.SecretKeySpec;
import java.util.*;

import static org.opencb.opencga.core.models.JwtPayload.FEDERATIONS;

public class JwtUtils {

    public static void validateJWTKey(String algorithmStr, String key) {
        SignatureAlgorithm algorithm = SignatureAlgorithm.forName(algorithmStr);
        SecretKeySpec secretKeySpec = new SecretKeySpec(key.getBytes(), algorithm.getJcaName());
        algorithm.assertValidSigningKey(secretKeySpec);
    }

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
                if (json.containsKey("exp")) {
                    Long exp = Long.parseLong(String.valueOf(json.get("exp")));
                    res = new Date(exp * 1000);
                } else {
                    // No expiration so adding 1 year to result
                    Calendar instance = Calendar.getInstance();
                    instance.setTime(res);
                    instance.add(Calendar.YEAR, 1);
                    res = instance.getTime();
                }
            } catch (ParseException e) {
                return new Date();
            }
     /*   SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        System.out.println(sdf.format(res));*/
        }
        return res;
    }

    public static List<JwtPayload.FederationJwtPayload> getFederations(Map<String, Object> claims) {
        List o = (List) claims.get(FEDERATIONS);
        if (CollectionUtils.isNotEmpty(o)) {
            List<JwtPayload.FederationJwtPayload> federationList = new ArrayList<>(o.size());
            for (Object federationObject : o) {
                if (federationObject instanceof Map) {
                    String id = ((Map<String, String>) federationObject).get("id");
                    List<String> projectIds = ((Map<String, List<String>>) federationObject).get("projectIds");
                    List<String> studyIds = ((Map<String, List<String>>) federationObject).get("studyIds");
                    federationList.add(new JwtPayload.FederationJwtPayload(id, projectIds, studyIds));
                }
            }
            return federationList;
        } else {
            return Collections.emptyList();
        }
    }
}
