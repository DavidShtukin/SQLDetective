package parser;

import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.SqlKind.AS;

public class NodeVisitor extends SqlShuttle {

    private class Scope {
        Namespace scopeNamespace = new Namespace();
    }

    private class Namespace {
        Map<String, Integer>                    tables        = new HashMap<>();      // tables used in current Select scope with count of usage
        Map<String, String>                     tableAliases  = new HashMap<>();      // key - alias, value - table
        Map<String, Map<String, ColumnMetrics>> tableColumns  = new HashMap<>();      // key - table, value - columns with metrics
        Set<String>                             columnAliases = new HashSet<>();      // cache them to skip later if used
        boolean                                 outerOrderBy  = false;
    }

    private final SqlDetective detective;

    private Deque<Scope>    scopes = new ArrayDeque<>();
    private Namespace       currentNamespace;
    private NodeContext     currentNodeContext;
    private JoinType        currentJoinMetricsType;

    private Map<String, List<String>> currentJoinConditions = new HashMap<>();
    private JoinConditionType currentJoinConditionType = JoinConditionType.NONE;

    public NodeVisitor(final SqlDetective detective) {
        this.detective = detective;
    }

    private void addTableToNamespace(final SqlNode node) {
        if (node instanceof SqlSelect) {
            node.accept(this);
        }
        if (node instanceof SqlIdentifier) {
            currentNamespace.tables.put(node.toString(), currentNamespace.tables.getOrDefault(node.toString(), 0) + 1);
        }
        else if (node instanceof SqlBasicCall && ((SqlBasicCall) node).getOperator().getKind() == AS) {
            String table = ((SqlBasicCall) node).operand(0).toString();
            currentNamespace.tables.put(table, currentNamespace.tables.getOrDefault(table, 0) + 1);
            currentNamespace.tableAliases.put(((SqlBasicCall) node).operand(1).toString(), table);
        }
    }

    private void addColumnAppearanceToNamespace(final SqlIdentifier node) {
        String column;
        if (node.isSimple()) {
            column = node.toString();
            if (currentNamespace.columnAliases.contains(column)) {
                return;
            }
            if (currentNamespace.tables.size() > 1 &&
                    (currentNodeContext != NodeContext.JOIN_KEY || !JoinConditionType.USING.equals(currentJoinConditionType))) {
                // create artificial group key combining all joined tables with '+' as a delimiter
                List<String> tables = currentNamespace.tables.keySet().stream().filter(table -> !table.contains("+")).collect(Collectors.toList());
                Collections.sort(tables);
                StringBuilder fakeTable = new StringBuilder();
                tables.forEach(table -> fakeTable.append(table).append("+"));
                fakeTable.deleteCharAt(fakeTable.length() - 1);
                String ft = fakeTable.toString();
                currentNamespace.tables.put(ft, currentNamespace.tables.getOrDefault(ft, 0) + 1);
                incrementColumnMetric(ft, column);
            }
            else {
                currentNamespace.tables.keySet().stream().forEach(table -> incrementColumnMetric(table, column));
                constructAndAddUsingJoinToLineage(column);
            }
        }
        else {
            // In case of Tab.col - first make sure to find the real table name rather than alias
            column = node.getComponent(1).toString();
            final String table = resolveTable(node.getComponent(0).toString());
            incrementColumnMetric(table, column);
        }
    }
    
    private void constructAndAddUsingJoinToLineage(final String column) {
        if (currentNamespace.tables.keySet().size() == 2) {
            List<String> tables = new ArrayList<>();
            tables.addAll(currentNamespace.tables.keySet());
            List<String> keys = currentNamespace.tables.keySet()
                                                       .stream()
                                                       .map(t -> t + "." + column)
                                                       .collect(Collectors.toList());
            currentJoinMetricsType = JoinType.EQUALITY;
            addJoinToLineage(tables, keys);
        }
    }

    private void addJoinToLineage(final List<String> tables, final List<String> keys) {
        Collections.sort(tables);
        String join = "(" + tables.get(0) + " -> " + tables.get(1) + ")";

        Collections.sort(keys);
        String keyCombination = keys.toString();

        incrementJoinMetrics(join, keyCombination);
    }

    private void incrementColumnMetric(final String table, final String column) {

        final Map<String, ColumnMetrics> tableColumnsMetrics = currentNamespace.tableColumns
                .computeIfAbsent(table, k -> new HashMap<String, ColumnMetrics>());

        final ColumnMetrics columnMetrics = tableColumnsMetrics.computeIfAbsent(column, k -> new ColumnMetrics());

        columnMetrics.counts.put(currentNodeContext, columnMetrics.counts.getOrDefault(currentNodeContext, 0) + 1);
    }

    private void addSelectScope() {
        scopes.push(new Scope());
        currentNamespace = scopes.peek().scopeNamespace;
        if (currentNodeContext == NodeContext.ORDER_BY) {
            currentNamespace.outerOrderBy = true;
        }
    }

    private void addOrderByScope() {
        currentNamespace.tableColumns.clear();
    }

    public void rollUpSelectScope() {
        if (scopes.size() > 0) {
            addScopeToLineage();
            if (!currentNamespace.outerOrderBy) {
                scopes.pop();
                if (scopes.size() > 0) {
                    currentNamespace = scopes.peek().scopeNamespace;
                }
            }
            else {
                Iterator scopesIterator = scopes.iterator();
                boolean take = false;
                while (scopesIterator.hasNext()) {
                    Scope scope = (Scope) scopesIterator.next();
                    if (take) {
                        Namespace ns = scope.scopeNamespace;
                        currentNamespace = scope.scopeNamespace;
                        break;
                    }
                    take = true;
                }
            }
        }
    }

    private void rollUpOrderByScope() {
        if (scopes.size() > 0) {
            addScopeToLineage();
            scopes.pop();
            currentNamespace = null;
        }
    }

    private void addScopeToLineage() {

        for (Map.Entry<String, Map<String, ColumnMetrics>> table : currentNamespace.tableColumns.entrySet()) {

            final ColumnsLineage tableLineage = detective.globalLineage.columnsLineage.computeIfAbsent(table.getKey(), k -> new ColumnsLineage());
            if (currentNodeContext != NodeContext.ORDER_BY) {
                tableLineage.tableCount += currentNamespace.tables.getOrDefault(table.getKey(), 0);
            }

            for (Map.Entry<String, ColumnMetrics> column : table.getValue().entrySet()) {

                final ColumnMetrics metrics = tableLineage.tableColumns.computeIfAbsent(column.getKey(),
                                                                                  k -> new ColumnMetrics());
                for (NodeContext context : column.getValue().counts.keySet()) {
                    metrics.counts.put(context, metrics.counts.getOrDefault(context, 0) + column.getValue().counts.get(context));
                }
            }
        }
    }

    private void handlePotentialJoin(SqlCall node) {
        currentNodeContext = NodeContext.JOIN_KEY;
        if ("=".equals(node.getOperator().getName())) {
            currentJoinMetricsType = JoinType.EQUALITY;
        }
        else {
            currentJoinMetricsType = JoinType.THETA;
        }
        constructAndAddJoinToLineage(node.getOperandList());
    }

    private List<String> extractTablesFromOperands(final SqlIdentifier op1, final SqlIdentifier op2) {
        List<String> tables = new ArrayList<>();
        if (!op1.isSimple() && !op2.isSimple()) {
            String table1 = resolveTable(op1.getComponent(0).toString());
            tables.add(table1);
            String table2 = resolveTable(op2.getComponent(0).toString());
            tables.add(table2);
        }
        else {
            tables.addAll(currentNamespace.tables.keySet());
        }
        return tables;
    }

    private void constructAndAddJoinToLineage(final List<SqlNode> joinCondition) {
        SqlIdentifier op1 = (SqlIdentifier) joinCondition.get(0);
        SqlIdentifier op2 = (SqlIdentifier) joinCondition.get(1);
        List<String> tables = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        if (!op1.isSimple() && !op2.isSimple()) {
            String table1 = resolveTable(op1.getComponent(0).toString());
            tables.add(table1);
            String table2 = resolveTable(op2.getComponent(0).toString());
            tables.add(table2);
            keys.add(table1 + "." + op1.getComponent(1).toString());
            keys.add(table2 + "." + op2.getComponent(1).toString());
        }
        else {
            tables.addAll(currentNamespace.tables.keySet());
            keys.add(op1.toString());
            keys.add(op2.toString());
        }
        addJoinToLineage(tables, keys);
    }

    private void incrementJoinMetrics(final String join, final String keyCombination) {
        JoinsLineage joinLineage = detective.globalLineage.joinsLineage.computeIfAbsent(join, k -> new JoinsLineage());
        joinLineage.joinCount++;

        final JoinMetrics joinMetrics = joinLineage.joinKeys.computeIfAbsent(keyCombination, k -> new JoinMetrics());
        
        joinMetrics.counts.put(currentJoinMetricsType, joinMetrics.counts.getOrDefault(currentJoinMetricsType,0) + 1);
    }

    private String resolveTable(final String alias) {
        String tableName = null;
        Iterator scopesIterator = scopes.iterator();
        while (scopesIterator.hasNext() && tableName == null) {
            Scope scope = (Scope) scopesIterator.next();
            tableName = scope.scopeNamespace.tableAliases.get(alias);
        }
        return tableName == null ? alias : tableName;
    }

    @Override
    public SqlNode visit(final SqlIdentifier node) {
        if (!"*".equals(node.toString())) {
            addColumnAppearanceToNamespace(node);
        }
        return node;
    }

    @Override
    public SqlNode visit(final SqlCall node) {

        SqlNodeList queryNodes = null;
        SqlNode tablesNode = null;
        SqlNode whereClause = null;
        SqlNode havingClause = null;

        switch (node.getKind()) {
            case SELECT:
                addSelectScope();
                tablesNode = ((SqlSelect) node).getFrom();
                if (tablesNode instanceof SqlIdentifier || tablesNode instanceof SqlBasicCall &&
                            ((SqlBasicCall) tablesNode).getOperator().getKind() == AS         &&
                            ((SqlBasicCall) tablesNode).operand(0) instanceof SqlIdentifier) {
                    addTableToNamespace(tablesNode);
                }
                else {
                    tablesNode.accept(this);
                }
                queryNodes = ((SqlSelect) node).getSelectList();
                whereClause = ((SqlSelect) node).getWhere();
                havingClause = ((SqlSelect) node).getHaving();

                currentNodeContext = NodeContext.SELECT;
                queryNodes.accept(this);

                if (whereClause != null) {
                    currentNodeContext = NodeContext.FILTER;
                    whereClause.accept(this);
                }

                if (havingClause != null) {
                    currentNodeContext = NodeContext.FILTER;
                    havingClause.accept(this);
                }

                final SqlNodeList groupByNodes = ((SqlSelect) node).getGroup();
                if (groupByNodes != null) {
                    currentNodeContext = NodeContext.GROUP_BY;
                    groupByNodes.accept(this);
                }
                rollUpSelectScope();
                break;
            case UPDATE:
                tablesNode = ((SqlUpdate) node).getTargetTable();
                addTableToNamespace(tablesNode);
                queryNodes = ((SqlUpdate) node).getTargetColumnList();
                whereClause = ((SqlUpdate) node).getCondition();
                queryNodes.accept(this);
                if (whereClause != null) {
                    whereClause.accept(this);
                }
                break;
            case ORDER_BY:
                for (SqlNode operand : node.getOperandList()) {
                    if (operand instanceof SqlNodeList) {
                        currentNodeContext = NodeContext.ORDER_BY;
                        addOrderByScope();
                        operand.accept(this);
                        rollUpOrderByScope();
                    }
                    else if (operand instanceof SqlSelect |
                             operand instanceof SqlWith) {
                        currentNodeContext = NodeContext.ORDER_BY;
                        operand.accept(this);
                    }
                }
                break;
            case JOIN:
                final SqlJoin joinNode = ((SqlJoin) node);
                SqlNode left = joinNode.getLeft();
                if (left instanceof SqlJoin ||
                        left instanceof SqlBasicCall && ((SqlBasicCall) left).operand(0) instanceof SqlSelect ) {
                    left.accept(this);
                }
                else {
                    addTableToNamespace(left);
                }
                final SqlNode right = joinNode.getRight();
                if (right instanceof SqlJoin ||
                        right instanceof SqlBasicCall && ((SqlBasicCall) right).operand(0) instanceof SqlSelect ) {
                    right.accept(this);
                }
                else {
                    addTableToNamespace(right);
                }

                currentJoinConditionType =  joinNode.getConditionType();
                final SqlNode condition = joinNode. getCondition();
                if (condition != null) {
                    currentNodeContext = NodeContext.JOIN_KEY;
                    condition.accept(this);
                }
                break;
            case AS:
                currentNamespace.columnAliases.add(node.operand(1).toString());
                node.operand(0).accept(this);
                break;
            case CASE:
                final SqlNode whenList = ((SqlCase) node).getWhenOperands();
                if (whenList != null) {
                    whenList.accept(this);
                }
                final SqlNode thenList = ((SqlCase) node).getThenOperands();
                if (thenList != null) {
                    thenList.accept(this);
                }
                final SqlNode elseList = ((SqlCase) node).getElseOperand();
                if (elseList != null) {
                    elseList.accept(this);
                }
                break;
            case WITH:
                node.operand(1).accept(this);
                break;
            default:
                final NodeContext prevContext = currentNodeContext;
                final String opName = (node).getOperator().getName();
                if ("COUNT".equals(opName) || "AVG".equals(opName) || "SUM".equals(opName) ||
                        "MIN".equals(opName) || "MAX".equals(opName)) {
                    currentNodeContext = NodeContext.AGGREGATION;
                }

                if (currentNodeContext == NodeContext.JOIN_KEY || currentNodeContext == NodeContext.FILTER) {
                    if (node.operandCount() == 2 &&
                            node.operand(0) instanceof SqlIdentifier && node.operand(1) instanceof SqlIdentifier) {
                        handlePotentialJoin(node);
                    }
                    else if (node.operandCount() == 2 &&
                            (node.operand(0) instanceof SqlIdentifier || node.operand(1) instanceof SqlIdentifier)) {
                        currentNodeContext = NodeContext.FILTER;
                    }
                }

                for (SqlNode operand : node.getOperandList()) {
                    operand.accept(this);
                }
                currentNodeContext = prevContext;
                break;
        }
        return node;
    }

    @Override
    public SqlNode visit(final SqlNodeList columnNodes) {
        for (SqlNode node : columnNodes.getList()) {
            node.accept(this);
        }
        return columnNodes;
    }
}
