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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Configuration management for the Code List Downloader application.
 */
public class Configuration {
    
    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);
    
    // Default values
    private static final String DEFAULT_REGISTRY_URL = 
        "https://ec.europa.eu/digital-building-blocks/sites/spaces/DIGITAL/pages/467108974/Registry+of+supporting+artefacts+to+implement+EN16931";
    private static final String DEFAULT_DOWNLOAD_BASE_PATH = "downloaded-files";
    private static final int DEFAULT_CHECK_INTERVAL_SECONDS = 86400; // 24 hours
    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 30000;
    private static final int DEFAULT_READ_TIMEOUT_MS = 60000;
    private static final int DEFAULT_DOWNLOAD_DELAY_SECONDS = 1; // 1 second between downloads
    private static final String DEFAULT_CSV_OUTPUT_BASE_PATH = "src/main/resources";
    private static final boolean DEFAULT_WRITE_LATEST_COPY = true;
    private static final boolean DEFAULT_AUTO_CONFIRM_DOWNLOADS = false;
    
    private String registryUrl;
    private String downloadBasePath;
    private int checkIntervalSeconds;
    private int connectTimeoutMs;
    private int readTimeoutMs;
    private int downloadDelaySeconds;  // Delay between downloads to appear human-like
    private String registryFilePath;   // Path for the file registry
    private String csvOutputBasePath;  // Base path for CSV outputs (default: src/main/resources)
    private boolean writeLatestCopy;   // Whether to write latest.csv copies for easy Git diffing
    private boolean autoConfirmDownloads;  // Skip interactive confirmation before downloads
    
    // Default constructor for Jackson
    public Configuration() {
        this.registryUrl = DEFAULT_REGISTRY_URL;
        this.downloadBasePath = DEFAULT_DOWNLOAD_BASE_PATH;
        this.checkIntervalSeconds = DEFAULT_CHECK_INTERVAL_SECONDS;
        this.connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
        this.readTimeoutMs = DEFAULT_READ_TIMEOUT_MS;
        this.downloadDelaySeconds = DEFAULT_DOWNLOAD_DELAY_SECONDS;
        this.csvOutputBasePath = DEFAULT_CSV_OUTPUT_BASE_PATH;
        this.writeLatestCopy = DEFAULT_WRITE_LATEST_COPY;
        this.autoConfirmDownloads = DEFAULT_AUTO_CONFIRM_DOWNLOADS;
    }
    
    /**
     * Loads configuration from config.json or creates default configuration.
     */
    public static Configuration load() throws IOException {
        File configFile = new File("config.json");
        
        if (configFile.exists() && configFile.length() > 0) {
            logger.info("Loading configuration from config.json");
            try {
                ObjectMapper mapper = new ObjectMapper();
                mapper.registerModule(new JavaTimeModule());
                return mapper.readValue(configFile, Configuration.class);
            } catch (IOException e) {
                logger.warn("Error reading config.json, using defaults: {}", e.getMessage());
            }
        }
        
        logger.info("No valid config.json found, using default configuration");
        Configuration config = new Configuration();
        
        // Save default configuration for user to edit
        if (!configFile.exists()) {
            config.save();
        }
        
        return config;
    }
    
    /**
     * Saves configuration to config.json.
     */
    public void save() throws IOException {
        save("config.json");
    }
    
    /**
     * Saves configuration to the specified file path.
     */
    public void save(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        File configFile = new File(filePath);
        mapper.writerWithDefaultPrettyPrinter()
              .writeValue(configFile, this);
        logger.info("Configuration saved to {}", filePath);
    }
    
    // Getters and setters
    public String getRegistryUrl() {
        return registryUrl;
    }
    
    public void setRegistryUrl(String registryUrl) {
        this.registryUrl = registryUrl;
    }
    
    public String getDownloadBasePath() {
        return downloadBasePath;
    }
    
    public void setDownloadBasePath(String downloadBasePath) {
        this.downloadBasePath = downloadBasePath;
    }
    
    public int getCheckIntervalSeconds() {
        return checkIntervalSeconds;
    }
    
    public void setCheckIntervalSeconds(int checkIntervalSeconds) {
        this.checkIntervalSeconds = checkIntervalSeconds;
    }
    
    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }
    
    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }
    
    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }
    
    public void setReadTimeoutMs(int readTimeoutMs) {
        this.readTimeoutMs = readTimeoutMs;
    }
    
    public int getDownloadDelaySeconds() {
        return downloadDelaySeconds;
    }
    
    public void setDownloadDelaySeconds(int downloadDelaySeconds) {
        this.downloadDelaySeconds = downloadDelaySeconds;
    }
    
    public String getCsvOutputBasePath() {
        return csvOutputBasePath;
    }
    
    public void setCsvOutputBasePath(String csvOutputBasePath) {
        this.csvOutputBasePath = csvOutputBasePath;
    }
    
    public boolean isWriteLatestCopy() {
        return writeLatestCopy;
    }
    
    public void setWriteLatestCopy(boolean writeLatestCopy) {
        this.writeLatestCopy = writeLatestCopy;
    }
    
    public boolean isAutoConfirmDownloads() {
        return autoConfirmDownloads;
    }
    
    public void setAutoConfirmDownloads(boolean autoConfirmDownloads) {
        this.autoConfirmDownloads = autoConfirmDownloads;
    }
    
    /**
     * Returns the path for the state file.
     */
    @JsonIgnore
    public String getStateFilePath() {
        return Paths.get(downloadBasePath, ".state.json").toString();
    }
    
    /**
     * Returns the path for the file registry (downloaded-files.json).
     * Located beside the downloaded-files directory, not inside it.
     * This keeps metadata separate from the actual downloaded files.
     */
    @JsonIgnore
    public String getRegistryFilePath() {
        if (registryFilePath != null) {
            return registryFilePath;
        }
        // Place downloaded-files.json beside the downloaded-files directory
        Path downloadBase = Paths.get(downloadBasePath);
        return downloadBase.getParent() != null 
            ? downloadBase.getParent().resolve("downloaded-files.json").toString()
            : Paths.get("downloaded-files.json").toString();
    }
    
    /**
     * Returns the path for the cumulative downloaded-files.csv.
     * Located beside downloaded-files.json, contains all files ever downloaded.
     */
    @JsonIgnore
    public String getDownloadedFilesCsvPath() {
        Path registryJson = Paths.get(getRegistryFilePath());
        return registryJson.getParent() != null
            ? registryJson.getParent().resolve("downloaded-files.csv").toString()
            : Paths.get("downloaded-files.csv").toString();
    }
    
    /**
     * Returns the path for Phase 1 CSV output directory.
     */
    @JsonIgnore
    public String getPhase1CsvPath() {
        return Paths.get(csvOutputBasePath, "phase1").toString();
    }
    
    /**
     * Returns the path for Phase 2 CSV output directory.
     */
    @JsonIgnore
    public String getPhase2CsvPath() {
        return Paths.get(csvOutputBasePath, "phase2").toString();
    }
    
    /**
     * Returns the path for Phase 3 CSV output directory.
     */
    @JsonIgnore
    public String getPhase3CsvPath() {
        return Paths.get(csvOutputBasePath, "phase3").toString();
    }
}

