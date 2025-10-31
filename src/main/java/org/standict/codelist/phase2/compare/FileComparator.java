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
package org.standict.codelist.phase2.compare;

import org.standict.codelist.shared.Configuration;
import org.standict.codelist.shared.FileMetadata;
import org.standict.codelist.shared.FileRegistry;
import org.standict.codelist.shared.CategoryDetector;
import org.standict.codelist.shared.CsvWriterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

/**
 * Phase 2: Compares current registry state with existing downloads.
 * Determines which files are NEW, CHANGED, or DELETED.
 * Writes diff CSV to src/main/resources/phase2/
 */
public class FileComparator {
    
    private static final Logger logger = LoggerFactory.getLogger(FileComparator.class);
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy");
    
    // CSV headers for Phase 2 diff - matches Phase 1 structure exactly (no download-specific fields)
    private static final String[] DIFF_HEADERS = {
        "effective_date", "publishing_date", "version", "category", "is_latest_release", "modification_date", "url", "filename", "filetype", "content_length", "content_type", "last_modified"
    };
    
    public enum ChangeType {
        NEW, CHANGED, DELETED, UNCHANGED
    }
    
    private final Configuration config;
    private final FileRegistry registry;
    
    public FileComparator(Configuration config, FileRegistry registry) {
        this.config = config;
        this.registry = registry;
    }
    
    /**
     * Compares current files with registry and determines which need to be downloaded.
     * Returns list of files that need downloading.
     */
    public List<FileMetadata> compareAndDetermineDownloads(List<FileMetadata> currentFiles) throws IOException {
        logger.info("Phase 2: Comparing {} files with registry", currentFiles.size());
        
        List<FileMetadata> filesToDownload = new ArrayList<>();
        List<DiffEntry> diffEntries = new ArrayList<>();
        
        // Check each current file
        for (FileMetadata current : currentFiles) {
            FileMetadata existing = registry.getFile(current.getUrl());
            String decodedFilename = decodeFilename(current.getFilename());
            
            // Check if file physically exists on disk
            boolean fileExistsOnDisk = false;
            String localPathToCheck = null;
            if (existing != null && existing.getLocalPath() != null) {
                localPathToCheck = existing.getLocalPath();
                fileExistsOnDisk = Files.exists(Paths.get(localPathToCheck));
            } else {
                // Try to determine expected path based on category
                String category = CategoryDetector.determineCategoryFromContext(current.getFilename(), current.getUrl());
                Path expectedPath = Paths.get(config.getDownloadBasePath(), category, decodedFilename);
                localPathToCheck = expectedPath.toString();
                fileExistsOnDisk = Files.exists(expectedPath);
            }
            
            // Determine if file needs download
            if (existing == null || !existing.isDownloaded() || !fileExistsOnDisk) {
                // NEW file (not in registry, not downloaded, or file missing from disk)
                filesToDownload.add(current);
                logger.info("NEW file: {} - File not in registry or missing from disk", decodedFilename);
            } else if (hasChanged(current, existing)) {
                // CHANGED file
                filesToDownload.add(current);
                logger.info("CHANGED file: {} - {}", decodedFilename, getChangeReason(current, existing));
            } else {
                // UNCHANGED file
                logger.debug("UNCHANGED file: {} (exists at: {})", decodedFilename, localPathToCheck);
            }
            
            String[] row = createDiffRow(current, localPathToCheck, fileExistsOnDisk);
            diffEntries.add(new DiffEntry(current, row));
        }
        
        // Filter out rows that already exist in Phase 3 CSV (already downloaded)
        Set<String> downloadedUrls = loadDownloadedUrlsFromPhase3();
        List<DiffEntry> filteredEntries = new ArrayList<>();
        for (DiffEntry entry : diffEntries) {
            if (!downloadedUrls.contains(entry.metadata.getUrl())) {
                filteredEntries.add(entry);
            } else {
                logger.debug("Skipping {} - already in Phase 3 CSV", entry.metadata.getFilename());
            }
        }
        
        // Write diff CSV (with sorting) - only rows that need download
        writeDiffCsv(filteredEntries);
        
        logger.info("Phase 2 complete. {} files need download ({} filtered from Phase 3 CSV)", 
            filteredEntries.size(), diffEntries.size() - filteredEntries.size());
        
        // Update filesToDownload to match filtered entries
        filesToDownload.clear();
        for (DiffEntry entry : filteredEntries) {
            filesToDownload.add(entry.metadata);
        }
        
        return filesToDownload;
    }
    
    /**
     * Checks if a file has changed compared to the existing version.
     */
    private boolean hasChanged(FileMetadata current, FileMetadata existing) {
        // Check content length
        if (current.getContentLength() > 0 && existing.getContentLength() > 0 &&
            current.getContentLength() != existing.getContentLength()) {
            return true;
        }
        
        // Check ETag
        if (current.getEtag() != null && existing.getEtag() != null &&
            !current.getEtag().equals(existing.getEtag())) {
            return true;
        }
        
        // Check last modified
        if (current.getLastModified() != null && existing.getLastModified() != null &&
            current.getLastModified().isAfter(existing.getLastModified())) {
            return true;
        }
        
        return false;
    }
    
    /**
     * Gets the reason for change.
     */
    private String getChangeReason(FileMetadata current, FileMetadata existing) {
        List<String> reasons = new ArrayList<>();
        
        if (current.getContentLength() > 0 && existing.getContentLength() > 0 &&
            current.getContentLength() != existing.getContentLength()) {
            reasons.add("size changed (" + existing.getContentLength() + " -> " + 
                       current.getContentLength() + ")");
        }
        
        if (current.getEtag() != null && existing.getEtag() != null &&
            !current.getEtag().equals(existing.getEtag())) {
            reasons.add("ETag changed");
        }
        
        if (current.getLastModified() != null && existing.getLastModified() != null &&
            current.getLastModified().isAfter(existing.getLastModified())) {
            reasons.add("last modified changed");
        }
        
        return String.join(", ", reasons);
    }
    
    /**
     * Creates a diff row matching Phase 1 structure plus actual_length and hash.
     */
    private String[] createDiffRow(FileMetadata current, String localPathToCheck, boolean fileExistsOnDisk) {
        // Format dates for CSV
        String effectiveDateStr = current.getEffectiveDate() != null ? 
            current.getEffectiveDate().format(DATE_FORMATTER) : "";
        String publishingDateStr = current.getPublishingDate() != null ? 
            current.getPublishingDate().format(DATE_FORMATTER) : "";
        String versionStr = current.getVersion() != null ? current.getVersion() : "";
        String category = CategoryDetector.determineCategoryFromContext(current.getFilename(), current.getUrl());
        String isLatestStr = current.isLatestRelease() ? "true" : "false";
        
        // Extract modification timestamp from URL and convert to date
        String modificationTimestamp = extractModificationTimestamp(current.getUrl());
        String modificationDate = convertUnixTimestampToDate(modificationTimestamp);
        
        // Extract filetype from filename
        String filetype = extractFiletype(current.getFilename());
        
        // Format last_modified
        String lastModifiedStr = current.getLastModified() != null ? 
            current.getLastModified().format(TIMESTAMP_FORMATTER) : "";
        
        return new String[]{
            effectiveDateStr,
            publishingDateStr,
            versionStr,
            category,
            isLatestStr,
            modificationDate,
            current.getUrl(),
            current.getFilename(),
            filetype,
            String.valueOf(current.getContentLength()),
            current.getContentType() != null ? current.getContentType() : "",
            lastModifiedStr
        };
    }
    
    /**
     * Extracts filetype from filename.
     */
    private static String extractFiletype(String filename) {
        if (filename == null || filename.isEmpty()) {
            return "";
        }
        int lastDot = filename.lastIndexOf('.');
        if (lastDot >= 0 && lastDot < filename.length() - 1) {
            return filename.substring(lastDot + 1).toUpperCase();
        }
        return "";
    }
    
    /**
     * Converts Unix timestamp (milliseconds) to formatted date string.
     */
    private static String convertUnixTimestampToDate(String timestampStr) {
        if (timestampStr == null || timestampStr.isEmpty()) {
            return "";
        }
        try {
            long timestampMillis = Long.parseLong(timestampStr);
            java.time.Instant instant = java.time.Instant.ofEpochMilli(timestampMillis);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(java.time.ZoneOffset.UTC);
            return instant.atZone(java.time.ZoneOffset.UTC).format(formatter);
        } catch (NumberFormatException e) {
            return "";
        }
    }
    
    /**
     * Helper class to hold FileMetadata and diff info for sorting
     */
    private static class DiffEntry {
        FileMetadata metadata;
        String[] row;
        
        DiffEntry(FileMetadata metadata, String[] row) {
            this.metadata = metadata;
            this.row = row;
        }
    }
    
    /**
     * Writes the diff CSV file with proper sorting (same as Phase 1: effective_date oldest first, category, filename).
     */
    private void writeDiffCsv(List<DiffEntry> diffEntries) throws IOException {
        // Sort by effective date (oldest first), then category, then filename
        diffEntries.sort(new Comparator<DiffEntry>() {
            @Override
            public int compare(DiffEntry e1, DiffEntry e2) {
                FileMetadata f1 = e1.metadata;
                FileMetadata f2 = e2.metadata;
                
                // First compare by effective date (oldest first - ascending order)
                // If effective date is null, use modification date from URL as fallback
                LocalDate date1 = f1.getEffectiveDate();
                LocalDate date2 = f2.getEffectiveDate();
                
                // Fallback to modification date if effective date is null
                if (date1 == null) {
                    date1 = extractModificationDateAsLocalDate(f1.getUrl());
                }
                if (date2 == null) {
                    date2 = extractModificationDateAsLocalDate(f2.getUrl());
                }
                
                if (date1 != null && date2 != null) {
                    int dateCompare = date1.compareTo(date2); // Oldest first
                    if (dateCompare != 0) {
                        return dateCompare;
                    }
                } else if (date1 != null) {
                    return -1; // f1 has date, f2 doesn't -> f1 comes first
                } else if (date2 != null) {
                    return 1;  // f2 has date, f1 doesn't -> f2 comes first
                }
                // If both null, continue to next sort criteria
                
                // If effective dates are equal or both null, compare by category (alphabetical)
                String category1 = CategoryDetector.determineCategoryFromContext(f1.getFilename(), f1.getUrl());
                String category2 = CategoryDetector.determineCategoryFromContext(f2.getFilename(), f2.getUrl());
                int categoryCompare = category1.compareTo(category2);
                if (categoryCompare != 0) {
                    return categoryCompare;
                }
                
                // If categories are equal, compare by filename (alphabetical)
                String filename1 = f1.getDecodedFilename().toLowerCase();
                String filename2 = f2.getDecodedFilename().toLowerCase();
                return filename1.compareTo(filename2);
            }
        });
        
        // Extract sorted rows
        List<String[]> sortedData = new ArrayList<>();
        for (DiffEntry entry : diffEntries) {
            sortedData.add(entry.row);
        }
        
        Path outputDir = Paths.get(config.getPhase2CsvPath());
        CsvWriterUtil.writeCsvWithLatestCopy(
            DIFF_HEADERS,
            sortedData,
            outputDir,
            "diff",
            config.isWriteLatestCopy()
        );
        
        logger.info("Wrote diff CSV to {}", outputDir);
    }
    
    /**
     * Extracts the modificationDate parameter from the URL.
     */
    private static String extractModificationTimestamp(String url) {
        // Extract the modificationDate parameter from the URL
        Pattern pattern = Pattern.compile("modificationDate=(\\d+)");
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }
    
    /**
     * Extracts modification date from URL and converts it to LocalDate for sorting.
     * Returns null if no modification date is found in URL.
     */
    private static LocalDate extractModificationDateAsLocalDate(String url) {
        String timestampStr = extractModificationTimestamp(url);
        if (timestampStr == null || timestampStr.isEmpty()) {
            return null;
        }
        try {
            long timestampMillis = Long.parseLong(timestampStr);
            java.time.Instant instant = java.time.Instant.ofEpochMilli(timestampMillis);
            return instant.atZone(java.time.ZoneOffset.UTC).toLocalDate();
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    /**
     * Decodes URL-encoded filename (%20 to spaces).
     */
    private static String decodeFilename(String filename) {
        try {
            return URLDecoder.decode(filename, StandardCharsets.UTF_8.toString());
        } catch (Exception e) {
            return filename;
        }
    }
    
    /**
     * Loads URLs from downloaded-files.csv (cumulative history) to filter out already downloaded files.
     * Returns a set of URLs that are already in the cumulative CSV.
     */
    private Set<String> loadDownloadedUrlsFromPhase3() {
        Set<String> downloadedUrls = new HashSet<>();
        // Use cumulative downloaded-files.csv instead of downloads-latest.csv
        Path cumulativePath = Paths.get(config.getDownloadedFilesCsvPath());
        
        if (!Files.exists(cumulativePath)) {
            logger.debug("Cumulative downloaded-files.csv not found at {} - will include all files in Phase 2", cumulativePath);
            return downloadedUrls;
        }
        
        try {
            // Read cumulative CSV - URL column is named "url"
            try (CSVParser parser = CSVParser.parse(cumulativePath, StandardCharsets.UTF_8, 
                    CSVFormat.DEFAULT.builder()
                        .setHeader()
                        .setSkipHeaderRecord(true)
                        .setQuoteMode(QuoteMode.ALL)
                        .build())) {
                
                for (CSVRecord record : parser) {
                    // Get URL from "url" column
                    try {
                        String url = record.get("url");
                        if (url != null && !url.isEmpty()) {
                            downloadedUrls.add(url);
                        }
                    } catch (IllegalArgumentException e) {
                        // Column doesn't exist, skip
                        logger.debug("URL column not found in downloaded-files.csv record: {}", e.getMessage());
                    }
                }
            }
            logger.info("Loaded {} URLs from cumulative downloaded-files.csv to filter Phase 2 results", downloadedUrls.size());
        } catch (IOException e) {
            logger.warn("Could not read cumulative downloaded-files.csv from {}: {} - will include all files in Phase 2", 
                cumulativePath, e.getMessage());
        }
        
        return downloadedUrls;
    }
    
}

