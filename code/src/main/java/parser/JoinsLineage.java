package parser;

import java.util.HashMap;
import java.util.Map;

public class JoinsLineage {

    public Integer                      joinCount;
    public Map<String, JoinMetrics>         joinKeys;

    public JoinsLineage() {
        joinCount = 0;
        joinKeys = new HashMap<>();
    }

    public JoinsLineage(final Integer joinCount,
                        final Map<String, JoinMetrics> joinKeys) {
        this.joinCount = joinCount;
        this.joinKeys = joinKeys;
    }
}
