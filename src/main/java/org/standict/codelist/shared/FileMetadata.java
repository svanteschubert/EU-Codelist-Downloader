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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Represents metadata for a file from the registry, including information
 * that can be obtained without downloading the file.
 */
public class FileMetadata {
    
    private String url;
    private String filename;  // Raw filename (may contain %20)
    private long contentLength;  // Size in bytes
    private String contentType;
    private LocalDateTime lastModified;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String etag;
    private transient String category; // Category determined from filename (eas, vatex-reason-codes, etc.)
    
    // Metadata extracted from HTML paragraphs
    @JsonProperty("effective_date")
    private LocalDate effectiveDate;      // Effective date from HTML (e.g., 15/05/25 -> 15.05.2025)
    
    @JsonProperty("publishing_date")
    private LocalDate publishingDate;   // Publishing date from HTML (e.g., 04/04/25 -> 04.04.2025)
    
    @JsonProperty("version")
    private String version;              // Version number extracted from HTML (e.g., "14" from "version 14.0")
    
    @JsonProperty("is_latest_release")
    private boolean isLatestRelease;    // Whether this is marked as "(latest version)" in HTML
    
    // Information after download
    private boolean downloaded;
    private LocalDateTime downloadTime;
    @JsonProperty("actual_hash")
    private String fileHash;  // SHA-256 hash
    @JsonProperty("actual_size")
    private long actualFileSize;
    private String localPath;
    
    /**
     * Default constructor for Jackson deserialization.
     */
    public FileMetadata() {
        this.downloaded = false;
    }
    
    /**
     * Constructor with URL for programmatic creation.
     */
    public FileMetadata(String url) {
        this.url = url;
        this.filename = extractFilename(url);
        this.downloaded = false;
    }
    
    private String extractFilename(String url) {
        try {
            String path = new java.net.URL(url).getPath();
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash >= 0 && lastSlash < path.length() - 1) {
                String filename = path.substring(lastSlash + 1);
                int questionMark = filename.indexOf('?');
                if (questionMark >= 0) {
                    return filename.substring(0, questionMark);
                }
                return filename;
            }
        } catch (Exception e) {
            // fallback handled below
        }
        return "file";
    }
    
    // Getters and Setters
    public String getUrl() {
        return url;
    }
    
    public void setUrl(String url) {
        this.url = url;
    }
    
    /**
     * Gets the raw filename (may contain URL encoding like %20).
     * For JSON output, use getDecodedFilename() instead.
     */
    @JsonIgnore
    public String getFilename() {
        return filename;
    }
    
    /**
     * Gets the decoded filename (spaces instead of %20) for JSON output.
     */
    @JsonProperty("filename")
    public String getDecodedFilename() {
        try {
            return URLDecoder.decode(filename, StandardCharsets.UTF_8.toString());
        } catch (Exception e) {
            return filename;
        }
    }
    
    public void setFilename(String filename) {
        this.filename = filename;
    }
    
    public long getContentLength() {
        return contentLength;
    }
    
    public void setContentLength(long contentLength) {
        this.contentLength = contentLength;
    }
    
    public String getContentType() {
        return contentType;
    }
    
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }
    
    public LocalDateTime getLastModified() {
        return lastModified;
    }
    
    public void setLastModified(LocalDateTime lastModified) {
        this.lastModified = lastModified;
    }
    
    public String getEtag() {
        return etag;
    }
    
    public void setEtag(String etag) {
        this.etag = etag;
    }
    
    public boolean isDownloaded() {
        return downloaded;
    }
    
    public void setDownloaded(boolean downloaded) {
        this.downloaded = downloaded;
    }
    
    public LocalDateTime getDownloadTime() {
        return downloadTime;
    }
    
    public void setDownloadTime(LocalDateTime downloadTime) {
        this.downloadTime = downloadTime;
    }
    
    public String getFileHash() {
        return fileHash;
    }
    
    public void setFileHash(String fileHash) {
        this.fileHash = fileHash;
    }
    
    public long getActualFileSize() {
        return actualFileSize;
    }
    
    public void setActualFileSize(long actualFileSize) {
        this.actualFileSize = actualFileSize;
    }
    
    public String getLocalPath() {
        return localPath;
    }
    
    public void setLocalPath(String localPath) {
        this.localPath = localPath;
    }
    
    /**
     * Gets the category for this file (calculated from filename).
     */
    @JsonProperty("category")
    public String getCategory() {
        if (category == null) {
            category = detectCategoryFromFilename();
        }
        return category;
    }
    
    /**
     * Sets the category for this file.
     */
    public void setCategory(String category) {
        this.category = category;
    }
    
    public LocalDate getEffectiveDate() {
        return effectiveDate;
    }
    
    public void setEffectiveDate(LocalDate effectiveDate) {
        this.effectiveDate = effectiveDate;
    }
    
    public LocalDate getPublishingDate() {
        return publishingDate;
    }
    
    public void setPublishingDate(LocalDate publishingDate) {
        this.publishingDate = publishingDate;
    }
    
    public String getVersion() {
        return version;
    }
    
    public void setVersion(String version) {
        this.version = version;
    }
    
    @JsonProperty("is_latest_release")
    public boolean isLatestRelease() {
        return isLatestRelease;
    }
    
    public void setLatestRelease(boolean isLatestRelease) {
        this.isLatestRelease = isLatestRelease;
    }
    
    /**
     * Detects category from filename (same logic as FileDownloader.detectCategory).
     */
    private String detectCategoryFromFilename() {
        String decodedFilename;
        try {
            decodedFilename = URLDecoder.decode(filename, StandardCharsets.UTF_8.toString());
        } catch (Exception e) {
            decodedFilename = filename;
        }
        
        String lowerFilename = decodedFilename.toLowerCase();
        
        if (lowerFilename.startsWith("en16931-ubl-") && lowerFilename.endsWith(".zip")) {
            return "validation-artefacts";
        }
        if (lowerFilename.startsWith("en16931-cii-") && lowerFilename.endsWith(".zip")) {
            return "validation-artefacts";
        }
        if (lowerFilename.contains("genericodes") && lowerFilename.endsWith(".zip")) {
            return "genericodes";
        }
        if (lowerFilename.contains("technical guidance") && lowerFilename.endsWith(".pdf")) {
            return "guidance";
        }
        if (lowerFilename.contains("exemption") && lowerFilename.endsWith(".xlsx")) {
            return "vatex-reason-codes";
        }
        if (lowerFilename.contains("address") && lowerFilename.contains("scheme") && lowerFilename.endsWith(".xlsx")) {
            return "eas";
        }
        if (lowerFilename.contains("en16931") && lowerFilename.contains("code lists") && lowerFilename.endsWith(".xlsx")) {
            return "en16931-code-lists";
        }
        return "other";
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileMetadata that = (FileMetadata) o;
        return Objects.equals(url, that.url);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(url);
    }
}

