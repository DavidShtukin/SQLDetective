package parser;

import java.util.HashMap;
import java.util.Map;

public enum JoinType {
    EQUALITY        (0),
    THETA           (1);

    private final int index;

    private static Map map = new HashMap<>();
    static {
        for (JoinType type : JoinType.values()) {
            map.put(type.index, type);
        }
    }

    public static JoinType valueOf(int index) {
        return (JoinType) map.get(index);
    }

    private JoinType(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }
}
