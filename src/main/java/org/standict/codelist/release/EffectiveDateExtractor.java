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
package org.standict.codelist.release;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.standict.codelist.shared.FileMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Extracts unique effective dates from downloaded-files.json registry.
 */
public class EffectiveDateExtractor {
    
    private static final Logger logger = LoggerFactory.getLogger(EffectiveDateExtractor.class);
    
    private final ObjectMapper objectMapper;
    
    public EffectiveDateExtractor() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }
    
    /**
     * Extracts all unique effective dates from the registry file.
     * Handles dates stored as arrays [YYYY, MM, DD] format.
     * 
     * @param registryFilePath Path to downloaded-files.json
     * @return Set of unique effective dates (sorted, oldest first)
     */
    public Set<LocalDate> extractUniqueEffectiveDates(String registryFilePath) throws IOException {
        Path registryPath = Paths.get(registryFilePath);
        if (!Files.exists(registryPath)) {
            throw new IOException("Registry file not found: " + registryFilePath);
        }
        
        String json = new String(Files.readAllBytes(registryPath), StandardCharsets.UTF_8);
        JsonNode rootNode = objectMapper.readTree(json);
        
        Set<LocalDate> effectiveDates = new TreeSet<>();

        // Support both array of entries and object map keyed by URL
        if (rootNode.isArray()) {
            for (JsonNode entry : rootNode) {
                JsonNode effectiveDateNode = entry.get("effective_date");
                if (effectiveDateNode != null && effectiveDateNode.isArray() && effectiveDateNode.size() >= 3) {
                    int year = effectiveDateNode.get(0).asInt();
                    int month = effectiveDateNode.get(1).asInt();
                    int day = effectiveDateNode.get(2).asInt();
                    try {
                        LocalDate date = LocalDate.of(year, month, day);
                        effectiveDates.add(date);
                    } catch (Exception e) {
                        logger.warn("Invalid date array [{}, {}, {}]: {}", year, month, day, e.getMessage());
                    }
                } else if (effectiveDateNode != null && !effectiveDateNode.isNull() && !effectiveDateNode.isArray()) {
                    try {
                        String dateStr = effectiveDateNode.asText();
                        if (dateStr != null && !dateStr.isEmpty()) {
                            LocalDate date = LocalDate.parse(dateStr);
                            effectiveDates.add(date);
                        }
                    } catch (Exception e) {
                        logger.debug("Could not parse effective_date: {}", effectiveDateNode, e);
                    }
                }
            }
        } else if (rootNode.isObject()) {
            rootNode.fields().forEachRemaining(entry -> {
                JsonNode valueNode = entry.getValue();
                JsonNode effectiveDateNode = valueNode.get("effective_date");
                if (effectiveDateNode != null && effectiveDateNode.isArray() && effectiveDateNode.size() >= 3) {
                    int year = effectiveDateNode.get(0).asInt();
                    int month = effectiveDateNode.get(1).asInt();
                    int day = effectiveDateNode.get(2).asInt();
                    try {
                        LocalDate date = LocalDate.of(year, month, day);
                        effectiveDates.add(date);
                    } catch (Exception e) {
                        logger.warn("Invalid date array in entry: {}", entry.getKey());
                    }
                } else if (effectiveDateNode != null && !effectiveDateNode.isNull() && !effectiveDateNode.isArray()) {
                    try {
                        String dateStr = effectiveDateNode.asText();
                        if (dateStr != null && !dateStr.isEmpty()) {
                            LocalDate date = LocalDate.parse(dateStr);
                            effectiveDates.add(date);
                        }
                    } catch (Exception e) {
                        logger.debug("Could not parse effective_date for entry {}: {}", entry.getKey(), effectiveDateNode, e);
                    }
                }
            });
        }
        
        logger.info("Found {} unique effective dates", effectiveDates.size());
        return effectiveDates;
    }
    
    /**
     * Filters registry entries by effective date.
     * Handles dates stored as arrays [YYYY, MM, DD] format.
     * 
     * @param registryFilePath Path to downloaded-files.json
     * @param effectiveDate The effective date to filter by
     * @return Map of URL to FileMetadata for entries matching the date
     */
    public Map<String, FileMetadata> filterByEffectiveDate(String registryFilePath, LocalDate effectiveDate) throws IOException {
        Path registryPath = Paths.get(registryFilePath);
        if (!Files.exists(registryPath)) {
            throw new IOException("Registry file not found: " + registryFilePath);
        }
        
        String json = new String(Files.readAllBytes(registryPath), StandardCharsets.UTF_8);
        JsonNode rootNode = objectMapper.readTree(json);
        
        Map<String, FileMetadata> filtered = new LinkedHashMap<>();
        
        // Iterate over all entries in the JSON object
        rootNode.fields().forEachRemaining(entry -> {
            String url = entry.getKey();
            JsonNode valueNode = entry.getValue();
            JsonNode effectiveDateNode = valueNode.get("effective_date");
            
            LocalDate entryDate = null;
            if (effectiveDateNode != null && effectiveDateNode.isArray() && effectiveDateNode.size() >= 3) {
                int year = effectiveDateNode.get(0).asInt();
                int month = effectiveDateNode.get(1).asInt();
                int day = effectiveDateNode.get(2).asInt();
                try {
                    entryDate = LocalDate.of(year, month, day);
                } catch (Exception e) {
                    logger.warn("Invalid date array in entry: {}", url);
                }
            } else if (effectiveDateNode != null && !effectiveDateNode.isNull() && !effectiveDateNode.isArray()) {
                try {
                    String dateStr = effectiveDateNode.asText();
                    if (dateStr != null && !dateStr.isEmpty()) {
                        entryDate = LocalDate.parse(dateStr);
                    }
                } catch (Exception e) {
                    // Ignore
                }
            }
            
            if (Objects.equals(entryDate, effectiveDate)) {
                try {
                    // Convert date arrays to ISO strings temporarily for Jackson deserialization
                    JsonNode modifiedNode = valueNode.deepCopy();
                    com.fasterxml.jackson.databind.node.ObjectNode objectNode = (com.fasterxml.jackson.databind.node.ObjectNode) modifiedNode;
                    
                    // Convert effective_date array to ISO string
                    if (effectiveDateNode != null && effectiveDateNode.isArray()) {
                        String isoDate = String.format("%04d-%02d-%02d", 
                            effectiveDateNode.get(0).asInt(),
                            effectiveDateNode.get(1).asInt(),
                            effectiveDateNode.get(2).asInt());
                        objectNode.put("effective_date", isoDate);
                    }
                    
                    // Convert publishing_date array to ISO string if present
                    JsonNode publishingDateNode = valueNode.get("publishing_date");
                    if (publishingDateNode != null && publishingDateNode.isArray() && publishingDateNode.size() >= 3) {
                        String pubIsoDate = String.format("%04d-%02d-%02d",
                            publishingDateNode.get(0).asInt(),
                            publishingDateNode.get(1).asInt(),
                            publishingDateNode.get(2).asInt());
                        objectNode.put("publishing_date", pubIsoDate);
                    }
                    
                    // Convert lastModified array to ISO LocalDateTime string for deserialization
                    JsonNode lastModifiedNode = valueNode.get("lastModified");
                    if (lastModifiedNode != null && lastModifiedNode.isArray() && lastModifiedNode.size() >= 6) {
                        // Format: [year, month, day, hour, minute, second, ...]
                        String lastModIso = String.format("%04d-%02d-%02dT%02d:%02d:%02d",
                            lastModifiedNode.get(0).asInt(),
                            lastModifiedNode.get(1).asInt(),
                            lastModifiedNode.get(2).asInt(),
                            lastModifiedNode.get(3).asInt(),
                            lastModifiedNode.get(4).asInt(),
                            lastModifiedNode.get(5).asInt());
                        objectNode.put("lastModified", lastModIso);
                    }
                    
                    // Convert downloadTime array to ISO LocalDateTime string for deserialization
                    JsonNode downloadTimeNode = valueNode.get("downloadTime");
                    if (downloadTimeNode != null && downloadTimeNode.isArray() && downloadTimeNode.size() >= 6) {
                        // Format: [year, month, day, hour, minute, second, nanoseconds]
                        String downloadTimeIso;
                        if (downloadTimeNode.size() >= 7) {
                            // Include nanoseconds in ISO format: "2025-10-29T20:23:48.218385400"
                            int nanos = downloadTimeNode.get(6).asInt();
                            downloadTimeIso = String.format("%04d-%02d-%02dT%02d:%02d:%02d.%09d",
                                downloadTimeNode.get(0).asInt(),
                                downloadTimeNode.get(1).asInt(),
                                downloadTimeNode.get(2).asInt(),
                                downloadTimeNode.get(3).asInt(),
                                downloadTimeNode.get(4).asInt(),
                                downloadTimeNode.get(5).asInt(),
                                nanos);
                        } else {
                            downloadTimeIso = String.format("%04d-%02d-%02dT%02d:%02d:%02d",
                                downloadTimeNode.get(0).asInt(),
                                downloadTimeNode.get(1).asInt(),
                                downloadTimeNode.get(2).asInt(),
                                downloadTimeNode.get(3).asInt(),
                                downloadTimeNode.get(4).asInt(),
                                downloadTimeNode.get(5).asInt());
                        }
                        objectNode.put("downloadTime", downloadTimeIso);
                    }
                    FileMetadata metadata = objectMapper.treeToValue(modifiedNode, FileMetadata.class);
                    // Ensure URL and effective date are set (they might not be in JSON)
                    metadata.setUrl(url);
                    metadata.setEffectiveDate(entryDate);
                    
                    filtered.put(url, metadata);
                } catch (Exception e) {
                    // Fallback: manually map all fields if Jackson fails
                    logger.warn("Could not deserialize entry for URL {}, using manual mapping: {}", url, e.getMessage());
                    try {
                        FileMetadata metadata = new FileMetadata(url);
                        
                        // Map all fields from JSON
                        if (valueNode.hasNonNull("filename")) {
                            metadata.setFilename(valueNode.get("filename").asText());
                        }
                        if (valueNode.hasNonNull("contentLength")) {
                            metadata.setContentLength(valueNode.get("contentLength").asLong());
                        }
                        if (valueNode.hasNonNull("contentType")) {
                            metadata.setContentType(valueNode.get("contentType").asText());
                        }
                        if (valueNode.hasNonNull("lastModified")) {
                            JsonNode lastModNode = valueNode.get("lastModified");
                            if (lastModNode.isArray() && lastModNode.size() >= 6) {
                                // Format: [year, month, day, hour, minute, second, ...]
                                LocalDateTime lastMod = LocalDateTime.of(
                                    lastModNode.get(0).asInt(),
                                    lastModNode.get(1).asInt(),
                                    lastModNode.get(2).asInt(),
                                    lastModNode.get(3).asInt(),
                                    lastModNode.get(4).asInt(),
                                    lastModNode.get(5).asInt());
                                metadata.setLastModified(lastMod);
                            } else if (!lastModNode.isNull()) {
                                String lastModStr = lastModNode.asText();
                                if (!lastModStr.isEmpty()) {
                                    metadata.setLastModified(LocalDateTime.parse(lastModStr));
                                }
                            }
                        }
                        if (valueNode.hasNonNull("etag")) {
                            metadata.setEtag(valueNode.get("etag").asText());
                        }
                        if (valueNode.hasNonNull("localPath")) {
                            metadata.setLocalPath(valueNode.get("localPath").asText());
                        }
                        if (valueNode.hasNonNull("downloaded")) {
                            metadata.setDownloaded(valueNode.get("downloaded").asBoolean());
                        } else {
                            metadata.setDownloaded(true);
                        }
                        if (valueNode.hasNonNull("downloadTime")) {
                            JsonNode downloadTimeNode = valueNode.get("downloadTime");
                            if (downloadTimeNode.isArray() && downloadTimeNode.size() >= 6) {
                                // Format: [year, month, day, hour, minute, second, nanoseconds]
                                LocalDateTime downloadTime = LocalDateTime.of(
                                    downloadTimeNode.get(0).asInt(),
                                    downloadTimeNode.get(1).asInt(),
                                    downloadTimeNode.get(2).asInt(),
                                    downloadTimeNode.get(3).asInt(),
                                    downloadTimeNode.get(4).asInt(),
                                    downloadTimeNode.get(5).asInt());
                                metadata.setDownloadTime(downloadTime);
                            } else if (!downloadTimeNode.isNull()) {
                                String downloadTimeStr = downloadTimeNode.asText();
                                if (!downloadTimeStr.isEmpty()) {
                                    metadata.setDownloadTime(LocalDateTime.parse(downloadTimeStr));
                                }
                            }
                        }
                        if (valueNode.hasNonNull("actual_hash")) {
                            metadata.setFileHash(valueNode.get("actual_hash").asText());
                        }
                        if (valueNode.hasNonNull("actual_size")) {
                            metadata.setActualFileSize(valueNode.get("actual_size").asLong());
                        }
                        if (valueNode.hasNonNull("publishing_date")) {
                            JsonNode pubDateNode = valueNode.get("publishing_date");
                            if (pubDateNode.isArray() && pubDateNode.size() >= 3) {
                                LocalDate pubDate = LocalDate.of(pubDateNode.get(0).asInt(), pubDateNode.get(1).asInt(), pubDateNode.get(2).asInt());
                                metadata.setPublishingDate(pubDate);
                            } else if (!pubDateNode.isNull()) {
                                metadata.setPublishingDate(LocalDate.parse(pubDateNode.asText()));
                            }
                        }
                        if (valueNode.hasNonNull("version")) {
                            metadata.setVersion(valueNode.get("version").asText());
                        }
                        if (valueNode.hasNonNull("is_latest_release")) {
                            metadata.setLatestRelease(valueNode.get("is_latest_release").asBoolean());
                        }
                        if (valueNode.hasNonNull("category")) {
                            metadata.setCategory(valueNode.get("category").asText());
                        }
                        
                        metadata.setEffectiveDate(entryDate);
                        filtered.put(url, metadata);
                    } catch (Exception e2) {
                        logger.error("Failed to manually map entry for URL {}: {}", url, e2.getMessage());
                    }
                }
            }
        });
        
        logger.info("Filtered {} entries for effective date {}", filtered.size(), effectiveDate);
        return filtered;
    }
}

