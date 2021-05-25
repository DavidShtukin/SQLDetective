package parser;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ColumnsLineage {

    public Integer                             tableCount;
    public Map<String, ColumnMetrics>          tableColumns;

    public ColumnsLineage() {
        tableCount = 0;
        tableColumns = new LinkedHashMap<>();
    }

    public ColumnsLineage(final Integer tableCount,
                          final Map<String, ColumnMetrics> tableColumns) {

        this.tableCount = tableCount;
        this.tableColumns = tableColumns;
    }
}
