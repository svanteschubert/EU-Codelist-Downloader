/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.standict.codelist.shared;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Utility class for writing CSV files with consistent formatting.
 */
public class CsvWriterUtil {
    
    private static final Logger logger = LoggerFactory.getLogger(CsvWriterUtil.class);
    
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    
    /**
     * Writes a CSV file with headers and data rows.
     * 
     * @param headers Array of column headers
     * @param data List of data rows (each row is an array of strings)
     * @param outputPath Path to the output CSV file
     * @throws IOException if writing fails
     */
    public static void writeCsv(String[] headers, List<String[]> data, Path outputPath) throws IOException {
        // Ensure parent directory exists
        Files.createDirectories(outputPath.getParent());
        
        CSVFormat format = CSVFormat.DEFAULT.builder()
            .setHeader(headers)
            .setQuoteMode(QuoteMode.ALL)
            .setRecordSeparator(System.lineSeparator())
            .build();

        try (CSVPrinter printer = new CSVPrinter(
                Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8),
                format)) {
            
            for (String[] row : data) {
                printer.printRecord((Object[]) row);
            }
            
            logger.debug("Wrote CSV file: {} with {} rows", outputPath, data.size());
        }
    }
    
    /**
     * Generates a timestamped filename.
     */
    public static String generateTimestampedFilename(String baseName) {
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMATTER);
        return String.format("%s-%s.csv", baseName, timestamp);
    }
    
    /**
     * Writes a CSV file and optionally a prefixed latest.csv copy.
     * 
     * @param headers Array of column headers
     * @param data List of data rows
     * @param outputDir Directory for the CSV file
     * @param baseFilename Base filename (without extension)
     * @param writeLatestCopy Whether to also write a prefixed latest.csv copy
     * @return Path to the written CSV file
     * @throws IOException if writing fails
     */
    public static Path writeCsvWithLatestCopy(String[] headers, List<String[]> data, 
                                              Path outputDir, String baseFilename, 
                                              boolean writeLatestCopy) throws IOException {
        // Generate timestamped filename
        String filename = generateTimestampedFilename(baseFilename);
        Path csvPath = outputDir.resolve(filename);
        
        // Write the timestamped CSV
        writeCsv(headers, data, csvPath);
        
        // Write prefixed latest.csv copy if requested (e.g., inventory-latest.csv, diff-latest.csv, downloads-latest.csv)
        if (writeLatestCopy) {
            String latestFilename = baseFilename + "-latest.csv";
            Path latestPath = outputDir.resolve(latestFilename);
            writeCsv(headers, data, latestPath);
            logger.debug("Wrote {} copy: {}", latestFilename, latestPath);
        }
        
        return csvPath;
    }
    
    /**
     * Converts a map to a string array row.
     * Useful for converting metadata maps to CSV rows.
     */
    public static String[] mapToRow(Map<String, Object> map, String[] headers) {
        String[] row = new String[headers.length];
        for (int i = 0; i < headers.length; i++) {
            Object value = map.get(headers[i]);
            row[i] = value != null ? value.toString() : "";
        }
        return row;
    }
    
    /**
     * Appends new rows to an existing CSV file, or creates it if it doesn't exist.
     * Preserves the existing header and data. Optionally sorts all rows after combining.
     * 
     * @param headers Array of column headers (must match existing file headers if file exists)
     * @param newRows List of new data rows to append
     * @param csvPath Path to the CSV file
     * @param comparator Optional comparator to sort all rows (existing + new) after combining.
     *                   If null, new rows are simply appended without sorting.
     * @throws IOException if reading or writing fails
     */
    public static void appendToCsv(String[] headers, List<String[]> newRows, Path csvPath, 
                                   Comparator<String[]> comparator) throws IOException {
        // Ensure parent directory exists
        Files.createDirectories(csvPath.getParent());
        
        // Check if file exists and has content
        boolean fileExists = Files.exists(csvPath) && Files.size(csvPath) > 0;
        
        List<String[]> allRows;
        
        if (fileExists) {
            // Read existing file to verify headers match
            List<String[]> existingRows = new ArrayList<>();
            try (CSVParser parser = CSVParser.parse(csvPath, StandardCharsets.UTF_8, 
                    CSVFormat.DEFAULT.builder()
                        .setHeader()
                        .setSkipHeaderRecord(true)
                        .setQuoteMode(QuoteMode.ALL)
                        .build())) {
                
                // Get existing headers
                List<String> existingHeaders = parser.getHeaderNames();
                
                // Verify headers match
                if (existingHeaders.size() != headers.length) {
                    throw new IOException(String.format(
                        "Header mismatch: existing file has %d columns, new data has %d columns",
                        existingHeaders.size(), headers.length));
                }
                
                // Read all existing rows
                for (CSVRecord record : parser) {
                    String[] row = new String[headers.length];
                    for (int i = 0; i < headers.length; i++) {
                        row[i] = record.get(i);
                    }
                    existingRows.add(row);
                }
            }
            
            // Combine existing and new rows
            allRows = new ArrayList<>(existingRows);
            allRows.addAll(newRows);
            
            // Sort if comparator provided
            if (comparator != null) {
                allRows.sort(comparator);
                logger.debug("Sorted all {} rows (existing + new) using provided comparator", allRows.size());
            }
            
            // Write complete file
            writeCsv(headers, allRows, csvPath);
            logger.info("Appended {} new rows to existing CSV file: {} (total: {} rows)", 
                newRows.size(), csvPath, allRows.size());
        } else {
            // File doesn't exist, create new file
            allRows = new ArrayList<>(newRows);
            
            // Sort if comparator provided
            if (comparator != null) {
                allRows.sort(comparator);
                logger.debug("Sorted {} new rows using provided comparator", allRows.size());
            }
            
            writeCsv(headers, allRows, csvPath);
            logger.info("Created new CSV file: {} with {} rows", csvPath, allRows.size());
        }
    }
    
    /**
     * Appends new rows to an existing CSV file without sorting.
     * Convenience method that calls appendToCsv with null comparator.
     */
    public static void appendToCsv(String[] headers, List<String[]> newRows, Path csvPath) throws IOException {
        appendToCsv(headers, newRows, csvPath, null);
    }
}

