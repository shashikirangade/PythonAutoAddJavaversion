package com.promoevaluator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

public class PromoApplication {

    // Function to get child records matching the conditions from the database
    public static List<Map<String, Object>> getMatchingChildRecords(List<Map<String, String>> attributes, Map<String, String> profileAttributes, List<Map<String, String>> products, Connection conn) throws SQLException {
        List<Map<String, Object>> matchingRecords = new ArrayList<>();
        try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM CHILD_RULES WHERE CONDITION = ? AND OPERATOR = '=' AND VALUE = ?")) {
            for (Map<String, String> attribute : attributes) {
                stmt.setString(1, attribute.get("Name"));
                stmt.setString(2, attribute.get("Value"));
                ResultSet rs = stmt.executeQuery();
                matchingRecords.addAll(getResults(rs));
            }

            for (Map.Entry<String, String> entry : profileAttributes.entrySet()) {
                stmt.setString(1, entry.getKey());
                stmt.setString(2, entry.getValue());
                ResultSet rs = stmt.executeQuery();
                matchingRecords.addAll(getResults(rs));
            }

            for (Map<String, String> product : products) {
                stmt.setString(1, "Product");
                stmt.setString(2, product.get("ProductId"));
                ResultSet rs = stmt.executeQuery();
                matchingRecords.addAll(getResults(rs));
            }
        }
        return matchingRecords;
    }

    // Function to get all child records associated with a specific source record ID
    public static List<Map<String, Object>> getChildRecordsBySourceId(String sourceRecordId, Connection conn) throws SQLException {
        List<Map<String, Object>> data;
        try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM CHILD_RULES WHERE SOURCE_RECORD_ID = ?")) {
            stmt.setString(1, sourceRecordId);
            ResultSet rs = stmt.executeQuery();
            data = getResults(rs);
        }
        return data;
    }

    // Function to get parent records from the database
    public static List<Map<String, Object>> getParentRecords(List<String> sourceRecordIds, Connection conn) throws SQLException {
        String placeholders = String.join(",", Collections.nCopies(sourceRecordIds.size(), "?"));
        String query = String.format("SELECT * FROM PARENT_COMP_MATRIX WHERE SOURCE_RECORD_ID IN (%s)", placeholders);

        List<Map<String, Object>> data;
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < sourceRecordIds.size(); i++) {
                stmt.setString(i + 1, sourceRecordIds.get(i));
            }
            ResultSet rs = stmt.executeQuery();
            data = getResults(rs);
        }
        return data;
    }

    // Function to get results from ResultSet
    private static List<Map<String, Object>> getResults(ResultSet rs) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                row.put(metaData.getColumnName(i), rs.getObject(i));
            }
            results.add(row);
        }
        return results;
    }

    // Operators mapping
    private static final Map<String, BiPredicate<String, String>> operators = new HashMap<>();
    static {
        operators.put("=", String::equals);
        operators.put("<>", (a, b) -> !a.equals(b));
        operators.put("LIKE", (a, b) -> a.contains(b));
        operators.put("!=", (a, b) -> !a.equals(b));
    }

    // Function to evaluate a condition
    public static boolean evaluateCondition(Map<String, Object> condition, List<Map<String, String>> attributes, Map<String, String> profileAttributes, List<Map<String, String>> products) {
        String attributeValue = null;
        if (condition.get("DATATYPE").toString().equalsIgnoreCase("attribute")) {
            for (Map<String, String> attribute : attributes) {
                if (attribute.get("Name").equals(condition.get("CONDITION"))) {
                    attributeValue = attribute.get("Value");
                    if (operators.get(condition.get("OPERATOR")).test(attributeValue, condition.get("VALUE").toString())) {
                        return true;
                    }
                }
            }
        } else if (condition.get("DATATYPE").toString().equalsIgnoreCase("profile attribute")) {
            attributeValue = profileAttributes.get(condition.get("CONDITION").toString());
            if (attributeValue != null && operators.get(condition.get("OPERATOR")).test(attributeValue, condition.get("VALUE").toString())) {
                return true;
            }
        } else if (condition.get("DATATYPE").toString().equalsIgnoreCase("product")) {
            for (Map<String, String> product : products) {
                if ("Product".equals(condition.get("CONDITION"))) {
                    attributeValue = product.get("ProductId");
                    if (operators.get(condition.get("OPERATOR")).test(attributeValue, condition.get("VALUE").toString())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // Function to build a valid boolean expression
    public static String buildExpression(String expression, Map<String, String> evalDict) {
        if (expression == null) {
            return "false";
        }
        List<String> tokens = new ArrayList<>(Arrays.asList(expression.split("\\s+")));
        List<String> result = new ArrayList<>();

        for (String token : tokens) {
            if (evalDict.containsKey(token)) {
                result.add(evalDict.get(token));
            } else if ("OR".equals(token)) {
                result.add("||");
            } else if ("AND".equals(token)) {
                result.add("&&");
            } else {
                result.add(token);
            }
        }

        long openParens = result.stream().filter(t -> "(".equals(t)).count();
        long closeParens = result.stream().filter(t -> ")".equals(t)).count();

        if (openParens > closeParens) {
            result.addAll(Collections.nCopies((int) (openParens - closeParens), ")"));
        } else if (closeParens > openParens) {
            result.addAll(0, Collections.nCopies((int) (closeParens - openParens), "("));
        }

        return String.join(" ", result);
    }
}