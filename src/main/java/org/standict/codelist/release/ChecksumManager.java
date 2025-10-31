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

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages checksums for release ZIP files.
 * Stores checksums in src/main/resources/releases/checksums.json
 */
public class ChecksumManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ChecksumManager.class);
    
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    
    private final Path checksumsFilePath;
    private final ObjectMapper objectMapper;
    private Map<String, String> checksums;  // Map of date string (YYYY-MM-DD) to checksum
    
    public ChecksumManager(String checksumsFilePath) {
        this.checksumsFilePath = Paths.get(checksumsFilePath);
        this.objectMapper = new ObjectMapper();
        this.checksums = new HashMap<>();
        loadChecksums();
    }
    
    /**
     * Loads checksums from file.
     */
    private void loadChecksums() {
        try {
            if (Files.exists(checksumsFilePath) && Files.size(checksumsFilePath) > 0) {
                String json = new String(Files.readAllBytes(checksumsFilePath), StandardCharsets.UTF_8);
                if (json.trim().isEmpty() || json.trim().equals("{}")) {
                    checksums = new HashMap<>();
                } else {
                    checksums = objectMapper.readValue(json, 
                        objectMapper.getTypeFactory().constructMapType(Map.class, String.class, String.class));
                }
                logger.info("Loaded {} checksums from {}", checksums.size(), checksumsFilePath);
            } else {
                checksums = new HashMap<>();
                logger.info("No existing checksums file found, starting fresh");
            }
        } catch (IOException e) {
            logger.error("Could not load checksums from {}: {}", checksumsFilePath, e.getMessage());
            checksums = new HashMap<>();
        }
    }
    
    /**
     * Saves checksums to file.
     */
    public void saveChecksums() throws IOException {
        Files.createDirectories(checksumsFilePath.getParent());
        String json = objectMapper.writerWithDefaultPrettyPrinter()
                                  .writeValueAsString(checksums);
        Files.write(checksumsFilePath, json.getBytes(StandardCharsets.UTF_8));
        logger.info("Saved {} checksums to {}", checksums.size(), checksumsFilePath);
    }
    
    /**
     * Calculates SHA-256 checksum of a file.
     */
    public String calculateFileChecksum(Path filePath) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] fileBytes = Files.readAllBytes(filePath);
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
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 algorithm not available", e);
        }
    }
    
    /**
     * Gets the stored checksum for a given effective date.
     * 
     * @param effectiveDate The effective date
     * @return The stored checksum, or null if not found
     */
    public String getStoredChecksum(LocalDate effectiveDate) {
        String dateKey = effectiveDate.format(DATE_FORMATTER);
        return checksums.get(dateKey);
    }
    
    /**
     * Updates the stored checksum for a given effective date.
     */
    public void updateChecksum(LocalDate effectiveDate, String checksum) {
        String dateKey = effectiveDate.format(DATE_FORMATTER);
        checksums.put(dateKey, checksum);
        logger.info("Updated checksum for {}: {}", dateKey, checksum);
    }
    
    /**
     * Checks if a ZIP file's checksum matches the stored checksum.
     * 
     * @param effectiveDate The effective date
     * @param zipFilePath Path to the ZIP file
     * @return true if checksums match or if no stored checksum exists, false if they differ
     */
    public boolean checksumMatches(LocalDate effectiveDate, Path zipFilePath) throws IOException {
        String storedChecksum = getStoredChecksum(effectiveDate);
        if (storedChecksum == null) {
            logger.info("No stored checksum for {}, will create/update release", effectiveDate);
            return false;
        }
        
        String calculatedChecksum = calculateFileChecksum(zipFilePath);
        boolean matches = storedChecksum.equals(calculatedChecksum);
        
        if (matches) {
            logger.info("Checksum matches for {} - skipping release update", effectiveDate);
        } else {
            logger.info("Checksum differs for {} (stored: {}, calculated: {}) - will update release", 
                effectiveDate, storedChecksum.substring(0, 8) + "...", calculatedChecksum.substring(0, 8) + "...");
        }
        
        return matches;
    }
}

