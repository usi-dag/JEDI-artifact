package s2s.experiments;

import s2s.bench_generator.MaybePlannedQuery;

import java.util.HashMap;
import java.util.Map;

public class Utils {

    public static Map<String, MaybePlannedQuery> unplanned(Map<String, String> queries) {
        Map<String, MaybePlannedQuery> result = new HashMap<>();
        for(Map.Entry<String, String> entry : queries.entrySet()) {
            String queryName = entry.getKey();
            String query = entry.getValue();
            result.put(queryName, new MaybePlannedQuery(query));
        }
        return result;
    }

}
