package com.promoevaluator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PromoApplicationPart2 {

    // Function to evaluate the final expression
    public static boolean evaluateExpression(String expression, List<Map<String, Object>> childRecords, List<Map<String, String>> attributes, Map<String, String> profileAttributes, List<Map<String, String>> products) {
        Map<String, String> evalDict = new HashMap<>();
        for (Map<String, Object> record : childRecords) {
            Object recordIdObj = record.get("ROW_ID");
            if (recordIdObj != null) {
                String recordId = recordIdObj.toString();
                evalDict.put(recordId, Boolean.toString(PromoApplication.evaluateCondition(record, attributes, profileAttributes, products)));
            } else {
                System.out.println("Null value encountered for RecordId in child record: " + record);
            }
        }

        String validExpression = PromoApplication.buildExpression(expression, evalDict);

        try {
            return Boolean.parseBoolean(validExpression);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // Function to apply promo
    public static List<Map<String, String>> applyPromo(List<Map<String, String>> attributes, Map<String, String> profileAttributes, List<Map<String, String>> products) throws SQLException {
        // Load the database file from the classpath
        String dbPath = PromoApplicationPart2.class.getClassLoader().getResource("mydatabase.db").getPath();
        dbPath = dbPath.replace("%20", " "); // To handle spaces in path
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            List<Map<String, Object>> matchingChildRecords = PromoApplication.getMatchingChildRecords(attributes, profileAttributes, products, conn);
            if (matchingChildRecords.isEmpty()) {
                return Collections.emptyList();
            }

            List<String> sourceRecordIds = matchingChildRecords.stream()
                .map(record -> {
                    Object sourceRecordIdObj = record.get("SOURCE_RECORD_ID");
                    if (sourceRecordIdObj != null) {
                        return sourceRecordIdObj.toString();
                    } else {
                        System.out.println("Null value encountered for SOURCE_RECORD_ID in child record: " + record);
                        return null;
                    }
                })
                .filter(id -> id != null)
                .distinct()
                .collect(Collectors.toList());

            List<Map<String, Object>> parentRecords = PromoApplication.getParentRecords(sourceRecordIds, conn);
            List<Map<String, String>> applicablePromos = new ArrayList<>();

            for (Map<String, Object> parentRecord : parentRecords) {
                String sourceRecordId = (String) parentRecord.get("SOURCE_RECORD_ID");
                String subjectEvaluator = (String) parentRecord.get("SUBJECT_EVALUATOR"); // assuming this is the correct key

                if (sourceRecordId == null || subjectEvaluator == null) {
                    System.out.println("Null value encountered in parent record. Skipping... " + parentRecord);
                    continue;
                }

                List<Map<String, Object>> childRecords = PromoApplication.getChildRecordsBySourceId(sourceRecordId, conn);

                for (Map<String, String> product : products) {
                    if (evaluateExpression(subjectEvaluator, childRecords, attributes, profileAttributes, Collections.singletonList(product))) {
                        String productList = (String) parentRecord.get("PARENT_PRODID");
                        if (product.get("ParentProdId") != null) {
                            if (productList != null && productList.equals(product.get("ParentProdId"))) {
                                applicablePromos.add(Map.of(
                                    "sourcerecordid", sourceRecordId,
                                    "rootproductid", product.get("ProductId"),
                                    "rowid", product.get("rowid")
                                ));
                            }
                        } else {
                            if (productList != null && productList.equals(product.get("ProductId"))) {
                                applicablePromos.add(Map.of(
                                    "sourcerecordid", sourceRecordId,
                                    "rootproductid", product.get("ProductId"),
                                    "rowid", product.get("rowid")
                                ));
                            }
                        }
                    }
                }
            }
            return applicablePromos;
        }
    }

    public static void main(String[] args) {
        // Start timing
        long startTime = System.nanoTime();

        // Hardcoded input for debugging
        List<Map<String, String>> attributes = Arrays.asList(
            Map.of("Name", "Action Code", "Value", "Add", "Type", "Attribute"),
            Map.of("Name", "Prod Prom Name", "Value", "Home fiber", "Type", "Attribute")
        );

        Map<String, String> profileAttributes = new HashMap<>();
        profileAttributes.put("BackEndOrderType", "WEBSHOP");
        profileAttributes.put("BGC_Partner_Sub_Segment", "E-Tail");

        List<Map<String, String>> products = Arrays.asList(
            Map.of("Name", "Mobile Voice", "ProductId", "MWLGG", "ParentProdId", "DUZZG", "rowid", "1-xx1"),
            Map.of("Name", "Telephony", "ProductId", "MWLGGI", "ParentProdId", "DRPVM", "rowid", "1-xx2")
        );

        try {
            List<Map<String, String>> promos = applyPromo(attributes, profileAttributes, products);
            System.out.println("Applicable promos: " + promos);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // End timing
        long endTime = System.nanoTime();

        // Calculate and print elapsed time
        long duration = (endTime - startTime) / 1_000_000; // Convert to milliseconds
        System.out.println("Execution time: " + duration + " ms");
    }
}