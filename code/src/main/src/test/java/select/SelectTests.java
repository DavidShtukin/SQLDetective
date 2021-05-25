package select;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import parser.ColumnsLineage;
import parser.LineageAnalysis;
import parser.NodeContext;
import parser.SqlDetective;

import org.junit.jupiter.api.Assertions;

public class SelectTests {

    private SqlDetective detective;

    @BeforeEach
    public void setup() {
         detective = new SqlDetective();
    }

    @Test
    public void columnQualified() throws SqlParseException {

        List<String> input = ImmutableList.of("SELECT tt.x1 FROM tab1 tt");

        LineageAnalysis resLineage = detective.parseInput(input);

        Assertions.assertEquals(1, resLineage.columnsLineage.entrySet().size());
        Assertions.assertEquals(1, resLineage.columnsLineage.get("TAB1").tableColumns.get("X1").counts.get(NodeContext.SELECT));

        detective.printLineage();
    }

    @Test
    public void columnUnqualifiedAndAlias() throws SqlParseException {

        List<String> input = ImmutableList.of("SELECT tt.x2 AS y1 FROM tab1 tt",
                                              "SELECT x1 AS y1, x2 AS y2, x3 FROM tab1");
        LineageAnalysis resLineage = detective.parseInput(input);

        Assertions.assertEquals(1, resLineage.columnsLineage.entrySet().size());
        Assertions.assertEquals(3, resLineage.columnsLineage.get("TAB1").tableColumns.size());
        Assertions.assertEquals(2, resLineage.columnsLineage.get("TAB1").tableColumns.get("X2").counts.get(NodeContext.SELECT));

        detective.printLineage();
    }

    @Test
    public void orderBy() throws SqlParseException {

        List<String> input = ImmutableList.of("SELECT i_date " +
                                              "  FROM tab1 " +
                                              "  ORDER BY i_date LIMIT 5 ");
        LineageAnalysis resLineage = detective.parseInput(input);

        int t = 5;

        Assertions.assertEquals(1, resLineage.columnsLineage.entrySet().size());
        Assertions.assertEquals(1, resLineage.columnsLineage.get("TAB1").tableColumns.size());
        Assertions.assertEquals(2, resLineage.columnsLineage.get("TAB1").tableColumns.get("I_DATE").counts.size());
        Assertions.assertEquals(1, resLineage.columnsLineage.get("TAB1").tableColumns.get("I_DATE").counts.get(NodeContext.SELECT));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("TAB1").tableColumns.get("I_DATE").counts.get(NodeContext.ORDER_BY));

        detective.printLineage();
    }



}
