package select;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
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

        Assertions.assertEquals(1, resLineage.columnsLineage.entrySet().size());
        Assertions.assertEquals(1, resLineage.columnsLineage.get("TAB1").tableColumns.size());
        Assertions.assertEquals(2, resLineage.columnsLineage.get("TAB1").tableColumns.get("I_DATE").counts.size());
        Assertions.assertEquals(1, resLineage.columnsLineage.get("TAB1").tableColumns.get("I_DATE").counts.get(NodeContext.SELECT));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("TAB1").tableColumns.get("I_DATE").counts.get(NodeContext.ORDER_BY));
        detective.printLineage();
    }

    @Test
    public void nestedSelectInWhere() throws SqlParseException {

        List<String> input = ImmutableList.of("Select a,b,c from tab2 where tab2.id in (select id from (select id from tab3))");
        LineageAnalysis resLineage = detective.parseInput(input);

        Assertions.assertEquals(2, resLineage.columnsLineage.entrySet().size());
        Assertions.assertEquals(1, resLineage.columnsLineage.get("TAB3").tableColumns.size());
        Assertions.assertEquals(4, resLineage.columnsLineage.get("TAB2").tableColumns.size());
        detective.printLineage();
    }

    @Test
    public void nestedSelectInFrom() throws SqlParseException {

        List<String> input = ImmutableList.of("Select a, b from (select a1, sum(b1) from tab1) as t1");
        LineageAnalysis resLineage = detective.parseInput(input);

        Assertions.assertEquals(1, resLineage.columnsLineage.get("TAB1").tableColumns.get("A1").counts.get(NodeContext.SELECT));
        Assertions.assertEquals(1, resLineage.columnsLineage.get("TAB1").tableColumns.get("B1").counts.get(NodeContext.AGGREGATION));
        detective.printLineage();
    }

    @Test
    public void caseClause() throws SqlParseException {

        List<String> input = ImmutableList.of("SELECT OrderID, OrderDate, " +
                "CASE " +
                "  WHEN Quantity > 30 THEN 'The quantity is greater than 30'" +
                "  WHEN Quantity = 30 THEN 'The quantity is 30' " +
                "ELSE 'The quantity is under 30' " +
                "END AS QuantityText " +
                "FROM OrderDetails");
        LineageAnalysis resLineage = detective.parseInput(input);

        Assertions.assertEquals(1, resLineage.columnsLineage.get("ORDERDETAILS").tableColumns.get("ORDERID").counts.get(NodeContext.SELECT));
        Assertions.assertEquals(2, resLineage.columnsLineage.get("ORDERDETAILS").tableColumns.get("QUANTITY").counts.get(NodeContext.SELECT));
        detective.printLineage();
    }
}