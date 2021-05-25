package parser;

import java.util.HashMap;
import java.util.Map;

public class JoinsLineage {

    public Integer                      joinCount;
    public Map<String, Integer>         joinKeys;

    public JoinsLineage() {
        joinCount = 0;
        joinKeys = new HashMap<>();
    }
}
