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
package org.standict.codelist.phase3.download;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpEntity;
import org.standict.codelist.shared.Configuration;
import org.standict.codelist.shared.FileMetadata;
import org.standict.codelist.shared.FileRegistry;
import org.standict.codelist.shared.CategoryDetector;
import org.standict.codelist.shared.CsvWriterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.io.Console;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Phase 3: Downloads files that need updating.
 * Writes download status CSV to src/main/resources/phase3/
 */
public class FileDownloader {
    
    private static final Logger logger = LoggerFactory.getLogger(FileDownloader.class);
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy");
    
    // CSV headers for Phase 3 downloads - matches Phase 1 structure plus last_modified, actual_length, hash
    private static final String[] DOWNLOAD_HEADERS = {
        "effective_date", "publishing_date", "version", "category", "is_latest_release", "modification_date", "url", "filename", "filetype", "content_length", "content_type", "last_modified", "actual_length", "hash"
    };
    
    private final Configuration config;
    private final CloseableHttpClient httpClient;
    private final FileRegistry registry;
    
    public FileDownloader(Configuration config, FileRegistry registry) {
        this.config = config;
        this.registry = registry;
        
        // Configure HTTP client with timeouts
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .setResponseTimeout(config.getReadTimeoutMs(), TimeUnit.MILLISECONDS)
                .build();
        
        this.httpClient = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build();
    }
    
    /**
     * Downloads a list of files with delays between downloads.
     * Also writes a CSV log of the download results.
     */
    public void downloadFiles(List<FileMetadata> filesToDownload) throws IOException, InterruptedException {
        downloadFiles(filesToDownload, false);
    }
    
    /**
     * Downloads a list of files with delays between downloads.
     * Also writes a CSV log of the download results.
     * 
     * @param filesToDownload List of files to download
     * @param skipConfirmation If true, skip interactive confirmation
     */
    public void downloadFiles(List<FileMetadata> filesToDownload, boolean skipConfirmation) throws IOException, InterruptedException {
        if (filesToDownload.isEmpty()) {
            logger.info("Phase 3: No files to download");
            List<String[]> emptyData = new ArrayList<>();
            List<FileMetadata> emptyFiles = new ArrayList<>();
            writeDownloadCsv(emptyFiles, emptyData);
            return;
        }
        
        logger.info("Phase 3: Starting download of {} files", filesToDownload.size());
        
        // Show summary of files to download
        showDownloadSummary(filesToDownload);
        
        // Interactive confirmation (unless skipped via config or parameter)
        if (!skipConfirmation && !config.isAutoConfirmDownloads()) {
            if (!confirmDownload()) {
                logger.info("Download cancelled by user");
                return;
            }
        }
        
        logger.info("Download base path: {}", config.getDownloadBasePath());
        
        List<String[]> downloadData = new ArrayList<>();
        List<FileMetadata> downloadedFiles = new ArrayList<>();
        
        for (int i = 0; i < filesToDownload.size(); i++) {
            FileMetadata metadata = filesToDownload.get(i);
            
            try {
                String status = downloadFile(metadata);
                downloadData.add(createDownloadRow(metadata, status));
                downloadedFiles.add(metadata);
                
                String decodedFilename = decodeFilename(metadata.getFilename());
                logger.info("Downloaded: {} ({})", decodedFilename, status);
                
                // Add delay between downloads (except for the last one)
                if (i < filesToDownload.size() - 1) {
                    int delaySeconds = config.getDownloadDelaySeconds();
                    logger.debug("Waiting {} seconds before next download...", delaySeconds);
                    Thread.sleep(delaySeconds * 1000L);
                }
            } catch (IOException e) {
                String decodedFilename = decodeFilename(metadata.getFilename());
                logger.error("Failed to download file {}: {}", 
                    decodedFilename, e.getMessage());
                downloadData.add(createDownloadRow(metadata, "FAILED: " + e.getMessage()));
                downloadedFiles.add(metadata);  // Add even on failure for sorting
            }
        }
        
        // Save registry after all downloads
        registry.saveRegistry();
        
        // Write download CSV (with sorting)
        writeDownloadCsv(downloadedFiles, downloadData);
        
        logger.info("Phase 3 complete. Downloaded {} files", 
            downloadData.stream().filter(row -> row.length > 16 && row[16].startsWith("SUCCEEDED")).count());
    }
    
    /**
     * Downloads a single file and updates its metadata with hash and timestamp.
     * Returns status string (SUCCEEDED or FAILED with reason).
     */
    private String downloadFile(FileMetadata metadata) throws IOException {
        // Decode filename (%20 to spaces)
        String decodedFilename = decodeFilename(metadata.getFilename());
        logger.info("Downloading file: {}", decodedFilename);
        
        // Determine target directory based on category
        String category = CategoryDetector.determineCategoryFromContext(metadata.getFilename(), metadata.getUrl());
        String targetDir = Paths.get(config.getDownloadBasePath(), category).toString();
        Files.createDirectories(Paths.get(targetDir));
        
        Path targetPath = Paths.get(targetDir, decodedFilename);
        
        // Download file
        HttpGet request = new HttpGet(metadata.getUrl());
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                throw new IOException("No content received from: " + metadata.getUrl());
            }
            
            try (InputStream inputStream = entity.getContent();
                 FileOutputStream outputStream = new FileOutputStream(targetPath.toFile())) {
                
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
            }
            
            // Calculate hash and update metadata
            String hash = FileRegistry.calculateFileHash(targetPath.toString());
            metadata.setFileHash(hash);
            metadata.setActualFileSize(Files.size(targetPath));
            metadata.setLocalPath(targetPath.toString());
            metadata.setDownloaded(true);
            metadata.setDownloadTime(LocalDateTime.now());
            
            // Ensure version is present; derive from filename if missing
            ensureVersionIfMissing(metadata);
            
            // Ensure category is set using CategoryDetector (consistent with Phase 1)
            // Category variable already defined above, so reuse it
            metadata.setCategory(category);
            
            // Register file in the registry
            registry.registerFile(metadata);
            
            String hashPreview = hash != null && hash.length() >= 8 ? 
                hash.substring(0, 8) + "..." : "unknown";
            
            // Show relative path from project root (normalize to use forward slashes for consistency)
            String relativePath = targetPath.toString().replace('\\', '/');
            logger.info("Download destination: {}", relativePath);
            logger.info("File saved successfully (hash: {})", hashPreview);
            
            return "SUCCEEDED";
        }
    }

    /**
     * Ensures the metadata has a version; if missing, derive from filename text.
     */
    private void ensureVersionIfMissing(FileMetadata metadata) {
        if (metadata.getVersion() != null && !metadata.getVersion().isEmpty()) {
            return;
        }
        String decodedFilename = decodeFilename(metadata.getFilename());
        String lower = decodedFilename.toLowerCase();
        
        // 1) Validation artefacts: en16931-ubl-1.3.15.zip / en16931-cii-1.3.15.zip
        Matcher mVal = Pattern.compile("en16931-(?:ubl|cii)-(\\d+(?:\\.\\d+)+)\\.zip").matcher(lower);
        if (mVal.find()) {
            metadata.setVersion(mVal.group(1));
            return;
        }
        
        // 2) Generic 'version X[.Y]' pattern in filename
        Matcher mNamed = Pattern.compile("(?:version|v)\\s*(\\d+(?:\\.\\d+)?)", Pattern.CASE_INSENSITIVE).matcher(decodedFilename);
        if (mNamed.find()) {
            String ver = mNamed.group(1);
            if (ver.endsWith(".0")) ver = ver.substring(0, ver.length() - 2);
            metadata.setVersion(ver);
            return;
        }
        
        // 3) EN16931 code lists vX in filename like 'values v9 - used from ...'
        Matcher mVX = Pattern.compile("\bv(\\d+)\b", Pattern.CASE_INSENSITIVE).matcher(decodedFilename);
        if (mVX.find()) {
            metadata.setVersion(mVX.group(1));
        }
    }
    
    /**
     * Creates a download row for CSV output.
     * Structure matches Phase 1 plus download-specific fields (downloaded_at, actual_size, actual_hash, local_path, status).
     */
    private String[] createDownloadRow(FileMetadata metadata, String status) {
        String filename = metadata.getFilename();
        String decodedFilename = decodeFilename(filename);
        String category = CategoryDetector.determineCategoryFromContext(filename, metadata.getUrl());
        
        // Extract filetype from filename
        String filetype = extractFiletype(filename);
        
        // Extract modification timestamp from URL and convert to date
        String modificationTimestamp = extractModificationTimestamp(metadata.getUrl());
        String modificationDate = convertUnixTimestampToDate(modificationTimestamp);
        
        // Format dates for CSV
        String effectiveDateStr = metadata.getEffectiveDate() != null ? 
            metadata.getEffectiveDate().format(DATE_FORMATTER) : "";
        String publishingDateStr = metadata.getPublishingDate() != null ? 
            metadata.getPublishingDate().format(DATE_FORMATTER) : "";
        String versionStr = metadata.getVersion() != null ? metadata.getVersion() : "";
        String isLatestStr = metadata.isLatestRelease() ? "true" : "false";
        
        // Format last_modified
        String lastModifiedStr = metadata.getLastModified() != null ? 
            metadata.getLastModified().format(TIMESTAMP_FORMATTER) : "";
        
        // Actual file size and hash from disk (populated after download)
        String actualLengthStr = metadata.getActualFileSize() > 0 ? 
            String.valueOf(metadata.getActualFileSize()) : "";
        String hashStr = metadata.getFileHash() != null ? metadata.getFileHash() : "";
        
        return new String[]{
            effectiveDateStr,
            publishingDateStr,
            versionStr,
            category,
            isLatestStr,
            modificationDate,
            metadata.getUrl(),
            filename,
            filetype,
            String.valueOf(metadata.getContentLength()),
            metadata.getContentType() != null ? metadata.getContentType() : "",
            lastModifiedStr,
            actualLengthStr,
            hashStr
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
     * Helper class to hold FileMetadata and row for sorting
     */
    private static class DownloadEntry {
        FileMetadata metadata;
        String[] row;
        
        DownloadEntry(FileMetadata metadata, String[] row) {
            this.metadata = metadata;
            this.row = row;
        }
    }
    
    /**
     * Writes the download CSV file with proper sorting (same as Phase 1: effective_date oldest first, category, filename).
     */
    private void writeDownloadCsv(List<FileMetadata> files, List<String[]> rows) throws IOException {
        // Create entries for sorting
        List<DownloadEntry> entries = new ArrayList<>();
        for (int i = 0; i < files.size() && i < rows.size(); i++) {
            entries.add(new DownloadEntry(files.get(i), rows.get(i)));
        }
        
        // Sort by effective date (oldest first), then category, then filename
        entries.sort(new Comparator<DownloadEntry>() {
            @Override
            public int compare(DownloadEntry e1, DownloadEntry e2) {
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
        for (DownloadEntry entry : entries) {
            sortedData.add(entry.row);
        }
        
        Path outputDir = Paths.get(config.getPhase3CsvPath());
        
        // Create comparator for CSV rows (effective_date oldest first with modification date fallback, category, filename)
        // CSV columns: effective_date(0), publishing_date(1), version(2), category(3), is_latest_release(4), 
        //              modification_date(5), url(6), filename(7), ...
        Comparator<String[]> csvRowComparator = (row1, row2) -> {
            // Compare by effective_date (index 0) - oldest first, with modification date fallback
            String date1Str = row1.length > 0 ? row1[0] : "";
            String date2Str = row2.length > 0 ? row2[0] : "";
            
            // If effective_date is empty, extract modification date from URL and use as fallback
            LocalDate date1 = parseDateFromString(date1Str);
            LocalDate date2 = parseDateFromString(date2Str);
            
            if (date1 == null && row1.length > 6) {
                date1 = extractModificationDateAsLocalDate(row1[6]); // URL at index 6
            }
            if (date2 == null && row2.length > 6) {
                date2 = extractModificationDateAsLocalDate(row2[6]); // URL at index 6
            }
            
            if (date1 != null && date2 != null) {
                int dateCompare = date1.compareTo(date2); // Oldest first
                if (dateCompare != 0) {
                    return dateCompare;
                }
            } else if (date1 != null) {
                return -1; // date1 has date, date2 doesn't -> date1 comes first
            } else if (date2 != null) {
                return 1;  // date2 has date, date1 doesn't -> date2 comes first
            }
            // If both null, continue to next sort criteria
            
            // Compare by category (index 3)
            String category1 = row1.length > 3 ? row1[3] : "";
            String category2 = row2.length > 3 ? row2[3] : "";
            int categoryCompare = category1.compareTo(category2);
            if (categoryCompare != 0) {
                return categoryCompare;
            }
            
            // Compare by filename (index 7)
            String filename1 = row1.length > 7 ? row1[7] : "";
            String filename2 = row2.length > 7 ? row2[7] : "";
            return filename1.toLowerCase().compareTo(filename2.toLowerCase());
        };
        
        // Write current run only to downloads-latest.csv (replace, not append - this is per-run snapshot)
        Path latestPath = outputDir.resolve("downloads-latest.csv");
        CsvWriterUtil.writeCsv(DOWNLOAD_HEADERS, sortedData, latestPath);
        logger.info("Wrote {} download records for this run to: {}", sortedData.size(), latestPath);
        
        // Append to cumulative downloaded-files.csv (contains all files ever downloaded)
        Path cumulativePath = Paths.get(config.getDownloadedFilesCsvPath());
        Files.createDirectories(cumulativePath.getParent());
        CsvWriterUtil.appendToCsv(DOWNLOAD_HEADERS, sortedData, cumulativePath, csvRowComparator);
        logger.info("Appended {} download records to cumulative: {}", sortedData.size(), cumulativePath);
        
        // Also create timestamped backup snapshot for current run
        String timestampedFilename = CsvWriterUtil.generateTimestampedFilename("downloads");
        Path timestampedPath = outputDir.resolve(timestampedFilename);
        CsvWriterUtil.writeCsv(DOWNLOAD_HEADERS, sortedData, timestampedPath);
        logger.info("Created timestamped backup: {}", timestampedPath);
    }
    
    /**
     * Parses a date string in "dd.MM.yyyy" format to LocalDate.
     * Returns null if parsing fails or string is empty.
     */
    private static LocalDate parseDateFromString(String dateStr) {
        if (dateStr == null || dateStr.isEmpty()) {
            return null;
        }
        try {
            return LocalDate.parse(dateStr, DATE_FORMATTER);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Compares two date strings in "dd.MM.yyyy" format.
     * Empty strings are treated as "oldest" (null dates).
     * Returns: negative if date1 < date2, positive if date1 > date2, 0 if equal.
     */
    private static int compareDates(String date1Str, String date2Str) {
        // Empty dates go first (oldest)
        if (date1Str == null || date1Str.isEmpty()) {
            if (date2Str == null || date2Str.isEmpty()) {
                return 0; // Both empty
            }
            return -1; // date1 empty comes before date2
        }
        if (date2Str == null || date2Str.isEmpty()) {
            return 1; // date2 empty comes after date1
        }
        
        try {
            LocalDate date1 = LocalDate.parse(date1Str, DATE_FORMATTER);
            LocalDate date2 = LocalDate.parse(date2Str, DATE_FORMATTER);
            return date1.compareTo(date2); // Oldest first
        } catch (Exception e) {
            // If parsing fails, compare as strings
            return date1Str.compareTo(date2Str);
        }
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
    private String decodeFilename(String filename) {
        try {
            return URLDecoder.decode(filename, StandardCharsets.UTF_8.toString());
        } catch (Exception e) {
            logger.warn("Failed to decode filename {}: {}", filename, e.getMessage());
            return filename;
        }
    }
    
    
    /**
     * Shows a summary of files that will be downloaded.
     */
    private void showDownloadSummary(List<FileMetadata> filesToDownload) {
        logger.info("=".repeat(80));
        logger.info("DOWNLOAD SUMMARY");
        logger.info("=".repeat(80));
        logger.info("Files to download: {}", filesToDownload.size());
        logger.info("");
        
        for (int i = 0; i < filesToDownload.size(); i++) {
            FileMetadata metadata = filesToDownload.get(i);
            String decodedFilename = decodeFilename(metadata.getFilename());
            String category = CategoryDetector.determineCategoryFromContext(metadata.getFilename(), metadata.getUrl());
            
            // Determine target path
            Path targetPath = Paths.get(config.getDownloadBasePath(), category, decodedFilename);
            String relativePath = targetPath.toString().replace('\\', '/');
            
            logger.info("{}. {} -> {}", 
                i + 1, 
                decodedFilename,
                relativePath);
        }
        
        logger.info("=".repeat(80));
    }
    
    /**
     * Prompts the user for confirmation before downloading.
     * Returns true if user confirms, false otherwise.
     */
    private boolean confirmDownload() {
        Console console = System.console();
        if (console != null) {
            // Interactive console available
            String response = console.readLine("Proceed with download? (y/n): ");
            return response != null && (response.trim().equalsIgnoreCase("y") || 
                                       response.trim().equalsIgnoreCase("yes"));
        } else {
            // Fallback for non-interactive environments (IDEs, batch scripts)
            logger.warn("Console not available, attempting to read from System.in");
            try (Scanner scanner = new Scanner(System.in)) {
                System.out.print("Proceed with download? (y/n): ");
                System.out.flush();
                String response = scanner.nextLine();
                return response != null && (response.trim().equalsIgnoreCase("y") || 
                                           response.trim().equalsIgnoreCase("yes"));
            } catch (Exception e) {
                logger.error("Error reading confirmation input: {}", e.getMessage());
                logger.info("Defaulting to NO - use --auto-confirm or set autoConfirmDownloads=true in config.json");
                return false;
            }
        }
    }
    
    /**
     * Closes the HTTP client.
     */
    public void close() {
        try {
            httpClient.close();
        } catch (IOException e) {
            logger.error("Error closing HTTP client: {}", e.getMessage());
        }
    }
}

