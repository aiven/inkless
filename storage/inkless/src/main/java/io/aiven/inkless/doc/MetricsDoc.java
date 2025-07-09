package io.aiven.inkless.doc;

import org.apache.kafka.common.utils.Time;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import io.aiven.inkless.control_plane.postgres.PostgresControlPlaneMetrics;

public class MetricsDoc {
    public static void main(String[] args) throws Exception {
        List<String[]> rows = new ArrayList<>();

        // Use reflection to find all QueryMetrics fields (instance fields)
        PostgresControlPlaneMetrics metricsInstance = new PostgresControlPlaneMetrics(Time.SYSTEM);
        for (Field field : PostgresControlPlaneMetrics.class.getDeclaredFields()) {
            // Check if the field is of type QueryMetrics (instance field)
            if (field.getType().getSimpleName().equals("QueryMetrics")) {
                field.setAccessible(true);
                Object queryMetrics = field.get(metricsInstance); // instance field
                if (queryMetrics == null) {
                    // Skip if the static field is not initialized
                    continue;
                }

                // Get histogram and gauge metric names from QueryMetrics fields
                String histogramMetric;
                String gaugeMetric;
                try {
                    Field queryTimeMetricNameField = queryMetrics.getClass().getDeclaredField("queryTimeMetricName");
                    queryTimeMetricNameField.setAccessible(true);
                    histogramMetric = (String) queryTimeMetricNameField.get(queryMetrics);
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException("field not found: queryTimeMetricName", e);
                }
                try {
                    Field queryRateMetricNameField = queryMetrics.getClass().getDeclaredField("queryRateMetricName");
                    queryRateMetricNameField.setAccessible(true);
                    gaugeMetric = (String) queryRateMetricNameField.get(queryMetrics);
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException("field not found: queryRateMetricName", e);
                }
                rows.add(new String[]{"", histogramMetric, ""});
                rows.add(new String[]{"", gaugeMetric, ""});
            }
        }

        // Write the .rst file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("docs/inkless/metrics.rst"))) {
            writer.write(".. _inkless_metrics:\n\n");
            writer.write("Inkless Metrics\n");
            writer.write("================\n\n");
            writer.write("+----------------+----------------------------------------------------------+--------------+\n");
            writer.write("| Description    | Mbean name                                               | Normal value |\n");
            writer.write("+================+==========================================================+==============+\n");
            for (String[] row : rows) {
                writer.write(String.format("| %-14s | %-56s | %-12s |\n", row[0], row[1], row[2]));
                writer.write("+----------------+----------------------------------------------------------+--------------+\n");
            }
        }
        System.out.println("metrics.rst generated.");
    }
}