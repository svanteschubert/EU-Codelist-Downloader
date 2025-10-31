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
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Imports entries from Phase 3 downloads CSV into the file registry when the registry is empty.
 */
public final class RegistryCsvImporter {

    private static final Logger logger = LoggerFactory.getLogger(RegistryCsvImporter.class);

    // CSV column names used by Phase 3 downloads CSV
    private static final String COL_EFFECTIVE_DATE = "effective_date";
    private static final String COL_PUBLISHING_DATE = "publishing_date";
    private static final String COL_VERSION = "version";
    private static final String COL_CATEGORY = "category";
    private static final String COL_IS_LATEST = "is_latest_release";
    private static final String COL_MODIFICATION_DATE = "modification_date"; // not used
    private static final String COL_URL = "url";
    private static final String COL_FILENAME = "filename";
    private static final String COL_FILETYPE = "filetype"; // not used
    private static final String COL_CONTENT_LENGTH = "content_length";
    private static final String COL_CONTENT_TYPE = "content_type";
    private static final String COL_LAST_MODIFIED = "last_modified";
    private static final String COL_ACTUAL_LENGTH = "actual_length";
    private static final String COL_HASH = "hash";

    private static final DateTimeFormatter DATE_DD_MM_YYYY = DateTimeFormatter.ofPattern("dd.MM.yyyy", Locale.ROOT);
    private static final DateTimeFormatter DATETIME_YYYY_MM_DD_HH_MM_SS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);

    private RegistryCsvImporter() { }

    /**
     * Seed the given registry from Phase 3 downloads-latest.csv if the CSV exists and the registry is empty.
     */
    public static void seedRegistryFromPhase3Csv(Configuration config, FileRegistry registry) {
        try {
            if (registry.getAllFiles() != null && !registry.getAllFiles().isEmpty()) {
                return; // already populated
            }

            // Use cumulative downloaded-files.csv instead of downloads-latest.csv
            Path cumulativeCsv = Paths.get(config.getDownloadedFilesCsvPath());
            if (!Files.exists(cumulativeCsv) || Files.size(cumulativeCsv) == 0) {
                logger.info("No cumulative downloaded-files.csv found at {} - skipping registry seeding", cumulativeCsv.toAbsolutePath());
                return;
            }

            logger.info("Seeding registry from cumulative downloaded-files.csv: {}", cumulativeCsv.toAbsolutePath());
            try (Reader reader = Files.newBufferedReader(cumulativeCsv, StandardCharsets.UTF_8);
                 CSVParser parser = CSVFormat.DEFAULT
                     .builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .build()
                     .parse(reader)) {

                for (CSVRecord record : parser) {
                    String url = record.get(COL_URL);
                    if (url == null || url.isEmpty()) {
                        continue;
                    }

                    FileMetadata meta = new FileMetadata(url);
                    // filename
                    safeSet(() -> meta.setFilename(record.get(COL_FILENAME)));
                    // sizes and types
                    safeSet(() -> meta.setContentLength(parseLong(record.get(COL_CONTENT_LENGTH))));
                    safeSet(() -> meta.setContentType(record.get(COL_CONTENT_TYPE)));
                    // dates
                    safeSet(() -> meta.setEffectiveDate(parseDate(record.get(COL_EFFECTIVE_DATE))));
                    safeSet(() -> meta.setPublishingDate(parseDate(record.get(COL_PUBLISHING_DATE))));
                    safeSet(() -> meta.setLastModified(parseDateTime(record.get(COL_LAST_MODIFIED))));
                    // version/latest
                    safeSet(() -> meta.setVersion(record.get(COL_VERSION)));
                    safeSet(() -> meta.setLatestRelease(parseBoolean(record.get(COL_IS_LATEST))));
                    // post-download info
                    meta.setDownloaded(true);
                    safeSet(() -> meta.setActualFileSize(parseLong(record.get(COL_ACTUAL_LENGTH))));
                    safeSet(() -> meta.setFileHash(record.get(COL_HASH)));

                    // Try to locate file on disk to set localPath
                    Path found = findDownloadedFile(config, record.get(COL_FILENAME));
                    if (found != null) {
                        meta.setLocalPath(found.toString());
                    }

                    registry.registerFile(meta);
                }
            }

            // Persist populated registry
            registry.saveRegistry();
            logger.info("Registry seeding from cumulative CSV complete ({} entries)", registry.getAllFiles().size());
        } catch (Exception e) {
            logger.warn("Could not seed registry from cumulative downloaded-files.csv: {}", e.getMessage());
        }
    }

    private static void safeSet(IORunnable r) {
        try { r.run(); } catch (Exception ignored) { }
    }

    private interface IORunnable { void run() throws Exception; }

    private static long parseLong(String s) {
        try { return s == null || s.isEmpty() ? 0L : Long.parseLong(s); } catch (Exception e) { return 0L; }
    }

    private static boolean parseBoolean(String s) {
        try { return s != null && s.equalsIgnoreCase("true"); } catch (Exception e) { return false; }
    }

    private static LocalDate parseDate(String s) {
        try { return (s == null || s.isEmpty()) ? null : LocalDate.parse(s, DATE_DD_MM_YYYY); } catch (Exception e) { return null; }
    }

    private static LocalDateTime parseDateTime(String s) {
        try { return (s == null || s.isEmpty()) ? null : LocalDateTime.parse(s, DATETIME_YYYY_MM_DD_HH_MM_SS); } catch (Exception e) { return null; }
    }

    private static Path findDownloadedFile(Configuration config, String filename) {
        if (filename == null || filename.isEmpty()) {
            return null;
        }
        try {
            Path base = Paths.get(config.getDownloadBasePath());
            if (!Files.exists(base)) {
                return null;
            }
            // Shallow search across known category folders
            try (var stream = Files.walk(base)) {
                return stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().equals(filename))
                    .findFirst()
                    .orElse(null);
            }
        } catch (IOException e) {
            return null;
        }
    }
}


