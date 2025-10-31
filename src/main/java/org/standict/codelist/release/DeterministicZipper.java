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

import org.standict.codelist.shared.FileMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Creates deterministic ZIP files with fixed timestamps and sorted entries.
 */
public class DeterministicZipper {
    
    private static final Logger logger = LoggerFactory.getLogger(DeterministicZipper.class);
    
    // Fixed timestamp for all ZIP entries to ensure determinism (1980-01-01 00:00:00 UTC)
    private static final long FIXED_TIMESTAMP = ZonedDateTime.of(1980, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"))
                                                               .toInstant().toEpochMilli();
    
    /**
     * Creates a deterministic ZIP file containing:
     * - Filtered downloaded-files.json
     * - Files from downloaded-files/ directory matching the effective date
     * - LICENSE file
     * - README-RELEASE.md file
     * 
     * @param filteredRegistryJson Path to the filtered downloaded-files.json to include
     * @param downloadedFilesBaseDir Base directory containing downloaded-files/
     * @param filteredEntries Map of entries for this effective date (to find matching files)
     * @param licensePath Path to LICENSE file
     * @param readmePath Path to README-RELEASE.md file
     * @param outputZipPath Path where to create the ZIP file
     */
    public void createDeterministicZip(
            Path filteredRegistryJson,
            Path downloadedFilesBaseDir,
            Map<String, FileMetadata> filteredEntries,
            Path licensePath,
            Path readmePath,
            Path outputZipPath) throws IOException {
        
        Files.createDirectories(outputZipPath.getParent());
        
        // Collect all files to include in ZIP (sorted by path for determinism)
        List<ZipEntryInfo> entries = new ArrayList<>();
        
        // Add filtered registry JSON
        entries.add(new ZipEntryInfo(
            "downloaded-files.json",
            filteredRegistryJson
        ));
        
        // Add LICENSE
        if (Files.exists(licensePath)) {
            entries.add(new ZipEntryInfo("LICENSE", licensePath));
        } else {
            logger.warn("LICENSE file not found at {}", licensePath);
        }
        
        // Add README-RELEASE.md
        if (Files.exists(readmePath)) {
            entries.add(new ZipEntryInfo("README-RELEASE.md", readmePath));
        } else {
            logger.warn("README-RELEASE.md file not found at {}", readmePath);
        }
        
        // Add files from downloaded-files/ directory
        for (FileMetadata metadata : filteredEntries.values()) {
            if (metadata.getLocalPath() != null && metadata.isDownloaded()) {
                Path localFile = Paths.get(metadata.getLocalPath());
                
                // Normalize separators for comparison
                String baseNorm = downloadedFilesBaseDir.toString().replace('\\', '/');
                String fileNorm = localFile.toString().replace('\\', '/');
                
                // If not absolute, align relative paths correctly under base
                if (!localFile.isAbsolute()) {
                    if (fileNorm.startsWith(baseNorm + "/")) {
                        // Already prefixed with base directory textually; use as-is
                        localFile = Paths.get(fileNorm);
                    } else if (fileNorm.startsWith("downloaded-files/")) {
                        // Strip leading logical folder and resolve under base
                        String stripped = fileNorm.substring("downloaded-files/".length());
                        localFile = downloadedFilesBaseDir.resolve(stripped);
                    } else {
                        // Generic relative path: resolve under base
                        localFile = downloadedFilesBaseDir.resolve(localFile);
                    }
                }
                
                if (Files.exists(localFile)) {
                    // Flatten structure: use just filename under downloaded-files/ (no subdirectories)
                    String filename = localFile.getFileName().toString();
                    String zipEntryName = "downloaded-files/" + filename;
                    entries.add(new ZipEntryInfo(zipEntryName, localFile));
                } else {
                    logger.warn("File not found: {}", localFile);
                }
            }
        }
        
        // Sort entries by path for determinism
        entries.sort(Comparator.comparing(e -> e.zipPath));
        
        // Create ZIP with deterministic entries
        try (ZipOutputStream zos = new ZipOutputStream(
                new BufferedOutputStream(Files.newOutputStream(outputZipPath)))) {
            
            for (ZipEntryInfo entryInfo : entries) {
                addZipEntry(zos, entryInfo.zipPath, entryInfo.filePath);
            }
        }
        
        logger.info("Created deterministic ZIP: {} ({} entries)", outputZipPath, entries.size());
    }
    
    /**
     * Adds a file entry to the ZIP with fixed timestamp.
     */
    private void addZipEntry(ZipOutputStream zos, String zipEntryName, Path filePath) throws IOException {
        ZipEntry entry = new ZipEntry(zipEntryName);
        entry.setTime(FIXED_TIMESTAMP);
        entry.setMethod(ZipOutputStream.DEFLATED);
        zos.putNextEntry(entry);
        
        try (InputStream is = Files.newInputStream(filePath)) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = is.read(buffer)) > 0) {
                zos.write(buffer, 0, len);
            }
        }
        
        zos.closeEntry();
    }
    
    /**
     * Helper class to track ZIP entry information.
     */
    private static class ZipEntryInfo {
        final String zipPath;
        final Path filePath;
        
        ZipEntryInfo(String zipPath, Path filePath) {
            this.zipPath = zipPath;
            this.filePath = filePath;
        }
    }
}

