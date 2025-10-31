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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Registry to track downloaded files with their hash signatures and metadata.
 */
public class FileRegistry {
    
    private static final Logger logger = LoggerFactory.getLogger(FileRegistry.class);
    
    private final String registryFilePath;
    private final ObjectMapper objectMapper;
    private Map<String, FileMetadata> registeredFiles;
    
    public FileRegistry(String registryFilePath) {
        this.registryFilePath = registryFilePath;
        this.objectMapper = new ObjectMapper();
        // Register JavaTimeModule to handle LocalDateTime serialization/deserialization
        this.objectMapper.registerModule(new JavaTimeModule());
        // Configure to exclude null fields (especially for eTag)
        this.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        this.registeredFiles = new HashMap<>();
        loadRegistry();
    }
    
    /**
     * Loads the registry from disk.
     */
    public void loadRegistry() {
        try {
            Path registryPath = Paths.get(registryFilePath);
            Path absolutePath = registryPath.toAbsolutePath();
            logger.info("Loading registry from: {} (absolute: {})", registryFilePath, absolutePath);
            
            if (Files.exists(registryPath) && Files.size(registryPath) > 0) {
                String json = new String(Files.readAllBytes(registryPath), StandardCharsets.UTF_8);
                registeredFiles = objectMapper.readValue(json, new TypeReference<Map<String, FileMetadata>>() {});
                long downloadedCount = registeredFiles.values().stream()
                    .filter(FileMetadata::isDownloaded)
                    .count();
                logger.info("Loaded {} files from registry ({} marked as downloaded)", 
                    registeredFiles.size(), downloadedCount);
            } else {
                registeredFiles = new HashMap<>();
                logger.info("No existing registry found at {} - starting fresh", absolutePath);
            }
        } catch (IOException e) {
            logger.error("Could not load registry from {}: {}", registryFilePath, e.getMessage());
            registeredFiles = new HashMap<>();
        }
    }
    
    /**
     * Saves the registry to disk with entries sorted by effective_date (oldest first), category, filename.
     */
    public void saveRegistry() {
        try {
            Path registryPath = Paths.get(registryFilePath);
            Path absolutePath = registryPath.toAbsolutePath();
            Files.createDirectories(registryPath.getParent());
            
            // Sort entries by effective_date (oldest first with modification date fallback), category, filename
            Map<String, FileMetadata> sortedFiles = registeredFiles.entrySet().stream()
                .sorted(new Comparator<Map.Entry<String, FileMetadata>>() {
                    @Override
                    public int compare(Map.Entry<String, FileMetadata> e1, Map.Entry<String, FileMetadata> e2) {
                        FileMetadata f1 = e1.getValue();
                        FileMetadata f2 = e2.getValue();
                        
                        // First compare by effective date (oldest first)
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
                })
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (e1, e2) -> e1,
                    LinkedHashMap::new
                ));
            
            String json = objectMapper.writerWithDefaultPrettyPrinter()
                                      .writeValueAsString(sortedFiles);
            Files.write(registryPath, json.getBytes(StandardCharsets.UTF_8));
            long downloadedCount = registeredFiles.values().stream()
                .filter(FileMetadata::isDownloaded)
                .count();
            logger.info("Saved registry with {} files ({} downloaded) to {}", 
                registeredFiles.size(), downloadedCount, absolutePath);
        } catch (IOException e) {
            logger.error("Could not save registry to {}: {}", registryFilePath, e.getMessage());
        }
    }
    
    /**
     * Extracts the modificationDate parameter from the URL and converts it to LocalDate.
     * Returns null if extraction or parsing fails.
     */
    private static LocalDate extractModificationDateAsLocalDate(String url) {
        if (url == null || url.isEmpty()) {
            return null;
        }
        String timestampStr = extractModificationTimestamp(url);
        if (timestampStr == null || timestampStr.isEmpty()) {
            return null;
        }
        try {
            long timestampMillis = Long.parseLong(timestampStr);
            Instant instant = Instant.ofEpochMilli(timestampMillis);
            return instant.atZone(ZoneOffset.UTC).toLocalDate();
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    /**
     * Extracts the modificationDate parameter from the URL.
     */
    private static String extractModificationTimestamp(String url) {
        Pattern pattern = Pattern.compile("modificationDate=(\\d+)");
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }
    
    /**
     * Gets metadata for a file if it's already in the registry.
     */
    public FileMetadata getFile(String url) {
        return registeredFiles.get(url);
    }
    
    /**
     * Registers a file after it's been downloaded.
     */
    public void registerFile(FileMetadata metadata) {
        registeredFiles.put(metadata.getUrl(), metadata);
    }
    
    /**
     * Calculates SHA-256 hash of a file.
     */
    public static String calculateFileHash(String filePath) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
            byte[] hashBytes = digest.digest(fileBytes);
            
            // Convert to hex string
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            
            return hexString.toString();
        } catch (NoSuchAlgorithmException | IOException e) {
            logger.error("Failed to calculate file hash: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Checks if a file needs to be downloaded.
     */
    public boolean needsDownload(FileMetadata newMetadata) {
        FileMetadata existing = getFile(newMetadata.getUrl());
        
        if (existing == null || !existing.isDownloaded()) {
            return true;
        }
        
        // Check if content length changed
        if (newMetadata.getContentLength() > 0 && existing.getContentLength() > 0 &&
            newMetadata.getContentLength() != existing.getContentLength()) {
            logger.info("File size changed: {} ({} -> {} bytes)", 
                newMetadata.getFilename(), existing.getContentLength(), newMetadata.getContentLength());
            return true;
        }
        
        // Check if ETag changed
        if (newMetadata.getEtag() != null && existing.getEtag() != null &&
            !newMetadata.getEtag().equals(existing.getEtag())) {
            logger.info("ETag changed: {}", newMetadata.getFilename());
            return true;
        }
        
        // Check if last modified changed
        if (newMetadata.getLastModified() != null && existing.getLastModified() != null &&
            newMetadata.getLastModified().isAfter(existing.getLastModified())) {
            logger.info("Last modified changed: {}", newMetadata.getFilename());
            return true;
        }
        
        return false;
    }
    
    /**
     * Gets all registered files.
     */
    @JsonIgnore
    public List<FileMetadata> getAllFiles() {
        return List.copyOf(registeredFiles.values());
    }
    
    /**
     * Removes a file from the registry.
     */
    public void removeFile(String url) {
        registeredFiles.remove(url);
    }
}

