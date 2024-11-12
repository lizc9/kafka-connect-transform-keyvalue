package org.radarbase.kafka.connect.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.util.Map;

/**
 * @author chenyuezi
 * @date 2023年12月20日 10:53
 */
public class JsonUtils {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectMapper mapperCC = new ObjectMapper();

    public static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {
    };

    static {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.SnakeCaseStrategy());
    }


    static {
        mapperCC.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapperCC.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static ObjectMapper getJsonMapper() {
        return mapper;
    }

    public static ObjectMapper getJsonMapperCC() {
        return mapperCC;
    }


    public static boolean isNull(JsonNode node) {
        return node == null || node.isNull();
    }

    public static boolean isNotNull(JsonNode node) {
        return !isNull(node);
    }

    public static boolean isEmptyString(JsonNode node) {
        return isNull(node) || (node.isTextual() && node.asText().isEmpty());
    }

    public static boolean isNotEmptyString(JsonNode node) {
        return !isEmptyString(node);
    }

}