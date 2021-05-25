package parser;

import java.util.HashMap;
import java.util.Map;

public enum NodeContext {
    SELECT          (0),
    AGGREGATION     (1),
    FILTER          (2),
    GROUP_BY        (3),
    JOIN_KEY        (4),
    ORDER_BY        (5);

    private final int index;

    private static Map map = new HashMap<>();
    static {
        for (NodeContext context : NodeContext.values()) {
            map.put(context.index, context);
        }
    }

    public static NodeContext valueOf(int index) {
        return (NodeContext) map.get(index);
    }

    private NodeContext(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }
}
