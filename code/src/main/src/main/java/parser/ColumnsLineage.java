package parser;

import java.util.HashMap;
import java.util.Map;

public class ColumnsLineage {

    public Integer                             tableCount;
    public Map<String, ColumnMetrics>          tableColumns;

    public ColumnsLineage() {
        tableCount = 0;
        tableColumns = new HashMap<>();
    }
}
