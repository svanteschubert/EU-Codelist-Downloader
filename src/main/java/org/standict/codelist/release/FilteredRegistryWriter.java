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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.standict.codelist.shared.FileMetadata;
import java.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Writes a filtered downloaded-files.json containing only entries for a specific effective date.
 */
public class FilteredRegistryWriter {
    
    private static final Logger logger = LoggerFactory.getLogger(FilteredRegistryWriter.class);
    
    private final ObjectMapper objectMapper;
    
    public FilteredRegistryWriter() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        // Default serialization uses ISO strings for LocalDateTime
        // We'll convert them to arrays manually after serialization
    }
    
    /**
     * Writes a filtered registry JSON file with entries sorted by category, then filename.
     * Updates localPath to reflect flattened structure in ZIP (no subdirectories).
     * 
     * @param filteredEntries Map of URL to FileMetadata for entries to include
     * @param outputPath Path where to write the filtered JSON
     */
    public void writeFilteredRegistry(Map<String, FileMetadata> filteredEntries, Path outputPath) throws IOException {
        // Create modified entries with flattened localPath
        Map<String, FileMetadata> modifiedEntries = new LinkedHashMap<>();
        for (Map.Entry<String, FileMetadata> entry : filteredEntries.entrySet()) {
            FileMetadata original = entry.getValue();
            
            // Create a copy with flattened localPath
            FileMetadata modified = new FileMetadata(original.getUrl());
            modified.setFilename(original.getFilename());
            modified.setContentLength(original.getContentLength());
            modified.setContentType(original.getContentType());
            modified.setLastModified(original.getLastModified());
            modified.setEtag(original.getEtag());
            modified.setCategory(original.getCategory());
            modified.setEffectiveDate(original.getEffectiveDate());
            modified.setPublishingDate(original.getPublishingDate());
            modified.setVersion(original.getVersion());
            modified.setLatestRelease(original.isLatestRelease());
            modified.setDownloaded(original.isDownloaded());
            modified.setDownloadTime(original.getDownloadTime());
            modified.setFileHash(original.getFileHash());
            modified.setActualFileSize(original.getActualFileSize());
            
            // Flatten localPath: remove category subdirectories, keep just filename under downloaded-files/
            if (original.getLocalPath() != null) {
                String originalPath = original.getLocalPath().replace('\\', '/');
                String filename = originalPath.substring(originalPath.lastIndexOf('/') + 1);
                // Use Windows-style backslashes for consistency with existing format
                modified.setLocalPath("downloaded-files\\" + filename.replace('/', '\\'));
            } else {
                modified.setLocalPath(null);
            }
            
            modifiedEntries.put(entry.getKey(), modified);
        }
        
        // Sort entries by category, then filename (maintaining order consistency)
        Map<String, FileMetadata> sortedEntries = modifiedEntries.entrySet().stream()
            .sorted((e1, e2) -> {
                FileMetadata f1 = e1.getValue();
                FileMetadata f2 = e2.getValue();
                
                // Compare by category
                String cat1 = f1.getCategory() != null ? f1.getCategory() : "";
                String cat2 = f2.getCategory() != null ? f2.getCategory() : "";
                int catCompare = cat1.compareTo(cat2);
                if (catCompare != 0) {
                    return catCompare;
                }
                
                // If categories are equal, compare by filename
                String filename1 = f1.getDecodedFilename() != null ? f1.getDecodedFilename().toLowerCase() : "";
                String filename2 = f2.getDecodedFilename() != null ? f2.getDecodedFilename().toLowerCase() : "";
                return filename1.compareTo(filename2);
            })
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (e1, e2) -> e1,
                LinkedHashMap::new
            ));
        
        Files.createDirectories(outputPath.getParent());
        
        // Serialize to JsonNode first
        JsonNode rootNode = objectMapper.valueToTree(sortedEntries);
        
        // Convert LocalDateTime fields (lastModified, downloadTime) from ISO strings to arrays
        if (rootNode.isObject()) {
            ObjectNode rootObject = (ObjectNode) rootNode;
            rootObject.fields().forEachRemaining(entry -> {
                JsonNode entryNode = entry.getValue();
                if (entryNode.isObject()) {
                    ObjectNode entryObject = (ObjectNode) entryNode;
                    convertDateTimeToArray(entryObject, "lastModified");
                    convertDateTimeToArray(entryObject, "downloadTime");
                }
            });
        }
        
        // Serialize the modified JsonNode to JSON string
        String json = objectMapper.writerWithDefaultPrettyPrinter()
                                  .writeValueAsString(rootNode);
        
        Files.write(outputPath, json.getBytes(StandardCharsets.UTF_8));
        
        logger.info("Wrote filtered registry with {} entries to {}", sortedEntries.size(), outputPath);
    }
    
    /**
     * Converts a LocalDateTime ISO string field to an array format [year, month, day, hour, minute, second].
     */
    private void convertDateTimeToArray(ObjectNode entryObject, String fieldName) {
        JsonNode fieldNode = entryObject.get(fieldName);
        if (fieldNode != null && !fieldNode.isNull() && fieldNode.isTextual()) {
            try {
                String isoString = fieldNode.asText();
                LocalDateTime dateTime = LocalDateTime.parse(isoString);
                
                // Create array: [year, month, day, hour, minute, second]
                ArrayNode arrayNode = objectMapper.createArrayNode();
                arrayNode.add(dateTime.getYear());
                arrayNode.add(dateTime.getMonthValue());
                arrayNode.add(dateTime.getDayOfMonth());
                arrayNode.add(dateTime.getHour());
                arrayNode.add(dateTime.getMinute());
                arrayNode.add(dateTime.getSecond());
                // Include nanoseconds if present (for downloadTime)
                int nanos = dateTime.getNano();
                if (nanos > 0) {
                    arrayNode.add(nanos);
                }
                
                entryObject.set(fieldName, arrayNode);
            } catch (Exception e) {
                // If parsing fails, leave as-is
                logger.debug("Could not convert {} to array format", fieldName, e);
            }
        }
    }
}

