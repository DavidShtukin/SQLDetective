package parser;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LineageAnalysis {

    public Map<String, ColumnsLineage>  columnsLineage;
    public Map<String, JoinsLineage>    joinsLineage;

    public Set<String> operatorsSeen;               // For testing coverage
    public Set<String> functionsSeen;               // For testing coverage

    public LineageAnalysis() {
        columnsLineage = new HashMap<>();
        joinsLineage = new HashMap<>();

        operatorsSeen = new HashSet<>();
        functionsSeen = new HashSet<>();
    }
}
