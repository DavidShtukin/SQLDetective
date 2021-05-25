package joins;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import parser.ColumnsLineage;
import parser.JoinsLineage;
import parser.LineageAnalysis;
import parser.NodeContext;
import parser.SqlDetective;

public class JoinsTests {

    private SqlDetective detective;

    @BeforeEach
    public void setup() {
         detective = new SqlDetective();
    }


    @Test
    public void unqualifiedColumnInUsingJoin() throws SqlParseException {

        List<String> input = ImmutableList.of("Select x, y FROM emp JOIN dept USING (depId)");
        LineageAnalysis resLineage = detective.parseInput(input);

        int t = 5;

//        Assertions.assertEquals(2, resLineage.entrySet().size());
//        Assertions.assertEquals(1, resLineage.get("TAB1").tableColumns.size());
//        Assertions.assertEquals(2, resLineage.get("TAB1").tableColumns.get("I_DATE").counts.size());
//        Assertions.assertEquals(1, resLineage.get("TAB1").tableColumns.get("I_DATE").counts.get(NodeContext.SELECT));
//        Assertions.assertEquals(1, resLineage.get("TAB1").tableColumns.get("I_DATE").counts.get(NodeContext.ORDER_BY));

        detective.printLineage();
    }

    @Test
    public void unqualifiedColumnsInUsingJoin() throws SqlParseException {

        List<String> input = ImmutableList.of("Select x, y FROM emp JOIN dept USING (depId, branchId)");
        LineageAnalysis resLineage = detective.parseInput(input);

        int t = 5;

        //        Assertions.assertEquals(2, resLineage.entrySet().size());
        //        Assertions.assertEquals(1, resLineage.get("TAB1").tableColumns.size());
        //        Assertions.assertEquals(2, resLineage.get("TAB1").tableColumns.get("I_DATE").counts.size());
        //        Assertions.assertEquals(1, resLineage.get("TAB1").tableColumns.get("I_DATE").counts.get(NodeContext.SELECT));
        //        Assertions.assertEquals(1, resLineage.get("TAB1").tableColumns.get("I_DATE").counts.get(NodeContext.ORDER_BY));

        detective.printLineage();
    }

    @Test
    public void qualifiedColumnsInOnJoin() throws SqlParseException {

        List<String> input = ImmutableList.of("SELECT tab1.x1, tab1.y1, tab2.z2, tab2.w2 FROM tab1 join tab2 ON tab1.x1 = tab2.x2 " +
                                                      "AND tab1.y1 = tab2.z2");
        LineageAnalysis resLineage = detective.parseInput(input);

        int t = 5;

        //        Assertions.assertEquals(2, resLineage.entrySet().size());
        //        Assertions.assertEquals(1, resLineage.get("TAB1").tableColumns.size());
        //        Assertions.assertEquals(2, resLineage.get("TAB1").tableColumns.get("I_DATE").counts.size());
        //        Assertions.assertEquals(1, resLineage.get("TAB1").tableColumns.get("I_DATE").counts.get(NodeContext.SELECT));
        //        Assertions.assertEquals(1, resLineage.get("TAB1").tableColumns.get("I_DATE").counts.get(NodeContext.ORDER_BY));

        detective.printLineage();
    }

    @Test
    public void qualifiedColumnsInMultipleOnJoins() throws SqlParseException {

        List<String> input = ImmutableList.of(
                "Select * from tb_A A " +
                        "join tb_B B on A.ID = B.ID " +
                        "join tb_C C on B.ID2 = C.ID2 " +
                        "join tb_D D on A.ID = D.ID and C.ID3 = D.ID3 and B.ID4 = D.ID4 " +
                        "where A.Foo = 6");

        LineageAnalysis resLineage = detective.parseInput(input);

        int t = 5;

        //        Assertions.assertEquals(2, resLineage.entrySet().size());
        //        Assertions.assertEquals(1, resLineage.get("TAB1").tableColumns.size());
        //        Assertions.assertEquals(2, resLineage.get("TAB1").tableColumns.get("I_DATE").counts.size());
        //        Assertions.assertEquals(1, resLineage.get("TAB1").tableColumns.get("I_DATE").counts.get(NodeContext.SELECT));
        //        Assertions.assertEquals(1, resLineage.get("TAB1").tableColumns.get("I_DATE").counts.get(NodeContext.ORDER_BY));

        detective.printLineage();
    }

    @Test
    public void sameTablesJoinsWithDifferentKeys() throws SqlParseException {

        List<String> input = ImmutableList.of("SELECT tab1.y1, tab2.y2 FROM tab1 join tab2 on tab1.x1 = tab2.x2",
                                              "SELECT tab1.x1, tab1.y1, tab2.z2, tab2.w2 FROM tab1 join tab2 on tab1.y1 = tab2.y2",
                                              "SELECT t1.x1, t2.z2 FROM tab1 t1 join tab2 t2 on t1.x1 = t2.x2",
                                              "SELECT t1.x1, t2.z2 FROM tab1 join tab2 using(x1)",
                                              "SELECT t1.x1, t2.z2 FROM tab1,tab2");
        LineageAnalysis resLineage = detective.parseInput(input);

        Assertions.assertEquals(1, resLineage.joinsLineage .entrySet().size());
        Assertions.assertEquals(4, resLineage.joinsLineage.get("TAB1 + TAB2").joinCount);
        Assertions.assertEquals(2, resLineage.joinsLineage.get("TAB1 + TAB2").joinKeys.get("[TAB1.X1, TAB2.X2]"));
        Assertions.assertEquals(1, resLineage.joinsLineage.get("TAB1 + TAB2").joinKeys.get("[X1]"));

        detective.printLineage();
    }

}
