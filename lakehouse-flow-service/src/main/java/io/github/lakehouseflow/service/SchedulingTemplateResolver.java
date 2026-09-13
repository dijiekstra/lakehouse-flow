package io.github.lakehouseflow.service;

import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves business-date variables in scheduling definitions and asset keys.
 */
@Service
public class SchedulingTemplateResolver {

    /**
     * Resolve supported date variables in one definition string.
     *
     * @param value source value
     * @param bizDate business date bound to the scheduling instance
     * @return resolved value, or null when the source is null
     */
    public String resolve(String value, LocalDate bizDate) {
        if (value == null || bizDate == null) {
            return value;
        }
        String date = bizDate.toString();
        return value.replace("${bizDate}", date).replace("${biz_date}", date);
    }

    /**
     * Resolve date variables recursively in a JSON-compatible scheduling policy.
     *
     * @param source source policy map
     * @param bizDate business date bound to the scheduling instance
     * @return detached map with resolved string values
     */
    public Map<String, Object> resolveMap(Map<String, Object> source, LocalDate bizDate) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> resolved = new LinkedHashMap<>();
        source.forEach((key, value) -> resolved.put(key, resolveValue(value, bizDate)));
        return resolved;
    }

    /**
     * Resolve one JSON-compatible value recursively.
     *
     * @param value source value
     * @param bizDate business date
     * @return resolved value
     */
    @SuppressWarnings("unchecked")
    private Object resolveValue(Object value, LocalDate bizDate) {
        if (value instanceof String text) {
            return resolve(text, bizDate);
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> typedMap = new LinkedHashMap<>();
            map.forEach((key, nestedValue) -> typedMap.put(String.valueOf(key), nestedValue));
            return resolveMap(typedMap, bizDate);
        }
        if (value instanceof List<?> list) {
            List<Object> resolved = new ArrayList<>(list.size());
            list.forEach(item -> resolved.add(resolveValue(item, bizDate)));
            return List.copyOf(resolved);
        }
        return value;
    }
}
