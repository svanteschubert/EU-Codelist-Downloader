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
import org.apache.hc.client5.http.classic.methods.*;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.FileEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.standict.codelist.shared.CategoryDetector;
import org.standict.codelist.shared.FileMetadata;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Manages GitHub releases using the GitHub REST API.
 */
public class GitHubReleaseManager {
    
    private static final Logger logger = LoggerFactory.getLogger(GitHubReleaseManager.class);
    
    private static final String GITHUB_API_BASE = "https://api.github.com";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    
    private final String owner;
    private final String repo;
    private final String token;
    private final ObjectMapper objectMapper;
    private final CloseableHttpClient httpClient;
    
    public GitHubReleaseManager(String owner, String repo, String token) {
        this.owner = owner;
        this.repo = repo;
        this.token = token;
        this.objectMapper = new ObjectMapper();
        this.httpClient = HttpClients.createDefault();
    }
    
    /**
     * Creates or updates a GitHub release for the given effective date.
     * 
     * @param effectiveDate The effective date
     * @param zipFilePath Path to the ZIP file to upload
     * @param zipFileName Name for the ZIP file in the release
     * @param filteredEntries Map of filtered entries for this effective date (for release notes)
     * @return true if successful, false otherwise
     */
    public boolean createOrUpdateRelease(LocalDate effectiveDate, Path zipFilePath, String zipFileName, Map<String, FileMetadata> filteredEntries) throws IOException {
        String tag = "release-" + effectiveDate.format(DATE_FORMATTER);
        String releaseTitle = "EU Code Lists - " + effectiveDate.format(DATE_FORMATTER) + " Effective Date";
        
        // Check if release already exists
        Long releaseId = getReleaseIdByTag(tag);
        
        // Calculate hash before creating/updating release
        String zipHash;
        try {
            zipHash = calculateFileHash(zipFilePath);
            logger.info("ZIP file hash (SHA-256): {}", zipHash);
        } catch (IOException e) {
            logger.warn("Failed to calculate ZIP hash: {}", e.getMessage());
            zipHash = null;
        }
        
        if (releaseId != null) {
            logger.info("Release with tag {} already exists (ID: {}), updating...", tag, releaseId);
            return updateRelease(releaseId, releaseTitle, zipFilePath, zipFileName, zipHash, filteredEntries, effectiveDate);
        } else {
            logger.info("Creating new release with tag {}", tag);
            return createRelease(tag, releaseTitle, zipFilePath, zipFileName, zipHash, filteredEntries, effectiveDate);
        }
    }
    
    /**
     * Checks if a release exists for the given effective date.
     * 
     * @param effectiveDate The effective date
     * @return true if release exists, false otherwise
     */
    public boolean releaseExists(LocalDate effectiveDate) throws IOException {
        String tag = "release-" + effectiveDate.format(DATE_FORMATTER);
        return getReleaseIdByTag(tag) != null;
    }
    
    /**
     * Gets the release ID for a given tag, or null if not found.
     */
    private Long getReleaseIdByTag(String tag) throws IOException {
        String url = String.format("%s/repos/%s/%s/releases/tags/%s", GITHUB_API_BASE, owner, repo, tag);
        
        HttpGet request = new HttpGet(url);
        addAuthHeader(request);
        request.setHeader("Accept", "application/vnd.github.v3+json");
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getCode();
            if (statusCode == 404) {
                return null;  // Release doesn't exist
            }
            
            if (statusCode != 200) {
                try {
                    String body = EntityUtils.toString(response.getEntity());
                    logger.error("Failed to get release by tag: {} - {}", statusCode, body);
                } catch (org.apache.hc.core5.http.ParseException pe) {
                    logger.error("Failed to get release by tag: {} - (could not parse body)", statusCode);
                }
                return null;
            }
            
            String body;
            try {
                body = EntityUtils.toString(response.getEntity());
            } catch (org.apache.hc.core5.http.ParseException pe) {
                throw new IOException("Failed to parse response body", pe);
            }
            JsonNode json = objectMapper.readTree(body);
            return json.get("id").asLong();
        }
    }
    
    /**
     * Creates a new GitHub release.
     */
    private boolean createRelease(String tag, String title, Path zipFilePath, String zipFileName, String zipHash, Map<String, FileMetadata> filteredEntries, LocalDate effectiveDate) throws IOException {
        String url = String.format("%s/repos/%s/%s/releases", GITHUB_API_BASE, owner, repo);
        
        HttpPost request = new HttpPost(url);
        addAuthHeader(request);
        request.setHeader("Accept", "application/vnd.github.v3+json");
        request.setHeader("Content-Type", "application/json");
        
        Map<String, Object> releaseData = new HashMap<>();
        releaseData.put("tag_name", tag);
        releaseData.put("name", title);
        releaseData.put("body", generateReleaseNotes(zipFilePath, zipHash, filteredEntries, effectiveDate));
        releaseData.put("draft", false);
        releaseData.put("prerelease", false);
        
        String jsonBody = objectMapper.writeValueAsString(releaseData);
        request.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getCode();
            if (statusCode < 200 || statusCode >= 300) {
                try {
                    String body = EntityUtils.toString(response.getEntity());
                    logger.error("Failed to create release: {} - {}", statusCode, body);
                } catch (org.apache.hc.core5.http.ParseException pe) {
                    logger.error("Failed to create release: {} - (could not parse body)", statusCode);
                }
                return false;
            }
            
            String body;
            try {
                body = EntityUtils.toString(response.getEntity());
            } catch (org.apache.hc.core5.http.ParseException pe) {
                throw new IOException("Failed to parse response body", pe);
            }
            JsonNode json = objectMapper.readTree(body);
            String uploadUrl = json.get("upload_url").asText();
            // Remove {?name,label} suffix from upload_url
            uploadUrl = uploadUrl.replaceAll("\\{\\?name,label\\}", "");
            
            logger.info("Release created successfully, uploading asset...");
            return uploadAsset(uploadUrl, zipFilePath, zipFileName);
        }
    }
    
    /**
     * Updates an existing GitHub release.
     */
    private boolean updateRelease(Long releaseId, String title, Path zipFilePath, String zipFileName, String zipHash, Map<String, FileMetadata> filteredEntries, LocalDate effectiveDate) throws IOException {
        String url = String.format("%s/repos/%s/%s/releases/%d", GITHUB_API_BASE, owner, repo, releaseId);
        
        HttpPatch request = new HttpPatch(url);
        addAuthHeader(request);
        request.setHeader("Accept", "application/vnd.github.v3+json");
        request.setHeader("Content-Type", "application/json");
        
        Map<String, Object> releaseData = new HashMap<>();
        releaseData.put("name", title);
        releaseData.put("body", generateReleaseNotes(zipFilePath, zipHash, filteredEntries, effectiveDate));
        
        String jsonBody = objectMapper.writeValueAsString(releaseData);
        request.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getCode();
            if (statusCode < 200 || statusCode >= 300) {
                try {
                    String body = EntityUtils.toString(response.getEntity());
                    logger.error("Failed to update release: {} - {}", statusCode, body);
                } catch (org.apache.hc.core5.http.ParseException pe) {
                    logger.error("Failed to update release: {} - (could not parse body)", statusCode);
                }
                return false;
            }
            
            String body;
            try {
                body = EntityUtils.toString(response.getEntity());
            } catch (org.apache.hc.core5.http.ParseException pe) {
                throw new IOException("Failed to parse response body", pe);
            }
            JsonNode json = objectMapper.readTree(body);
            String uploadUrl = json.get("upload_url").asText();
            uploadUrl = uploadUrl.replaceAll("\\{\\?name,label\\}", "");
            
            logger.info("Release updated successfully, uploading asset...");
            return uploadAsset(uploadUrl, zipFilePath, zipFileName);
        }
    }
    
    /**
     * Uploads a ZIP file as a release asset.
     * If an asset with the same name exists, it will be deleted first.
     */
    private boolean uploadAsset(String uploadUrl, Path zipFilePath, String zipFileName) throws IOException {
        // Extract release ID from upload URL to get assets
        // uploadUrl format: https://uploads.github.com/repos/owner/repo/releases/{id}/assets
        try {
            // Get release ID from upload URL and fetch release to get assets
            String releasesUrl = String.format("%s/repos/%s/%s/releases", GITHUB_API_BASE, owner, repo);
            HttpGet getReleases = new HttpGet(releasesUrl);
            addAuthHeader(getReleases);
            getReleases.setHeader("Accept", "application/vnd.github.v3+json");
            
            try (CloseableHttpResponse response = httpClient.execute(getReleases)) {
                if (response.getCode() == 200) {
                    String body;
                    try {
                        body = EntityUtils.toString(response.getEntity());
                    } catch (org.apache.hc.core5.http.ParseException pe) {
                        throw new IOException("Failed to parse releases list", pe);
                    }
                    JsonNode releases = objectMapper.readTree(body);
                    
                    // Find release that matches upload URL by checking upload_url field
                    for (JsonNode release : releases) {
                        String releaseUploadUrl = release.get("upload_url").asText().replaceAll("\\{\\?name,label\\}", "");
                        if (uploadUrl.equals(releaseUploadUrl)) {
                            // Found the release, get its assets
                            long releaseId = release.get("id").asLong();
                            String assetsUrl = String.format("%s/repos/%s/%s/releases/%d/assets", 
                                GITHUB_API_BASE, owner, repo, releaseId);
                            HttpGet getAssets = new HttpGet(assetsUrl);
                            addAuthHeader(getAssets);
                            getAssets.setHeader("Accept", "application/vnd.github.v3+json");
                            
                            try (CloseableHttpResponse assetsResponse = httpClient.execute(getAssets)) {
                                if (assetsResponse.getCode() == 200) {
                                    String assetsBody;
                                    try {
                                        assetsBody = EntityUtils.toString(assetsResponse.getEntity());
                                    } catch (org.apache.hc.core5.http.ParseException pe) {
                                        throw new IOException("Failed to parse assets list", pe);
                                    }
                                    JsonNode assets = objectMapper.readTree(assetsBody);
                                    for (JsonNode asset : assets) {
                                        if (zipFileName.equals(asset.get("name").asText())) {
                                            long assetId = asset.get("id").asLong();
                                            String deleteAssetUrl = String.format("%s/repos/%s/%s/releases/assets/%d", 
                                                GITHUB_API_BASE, owner, repo, assetId);
                                            HttpDelete deleteRequest = new HttpDelete(deleteAssetUrl);
                                            addAuthHeader(deleteRequest);
                                            deleteRequest.setHeader("Accept", "application/vnd.github.v3+json");
                                            try (CloseableHttpResponse deleteResponse = httpClient.execute(deleteRequest)) {
                                                logger.info("Deleted existing asset: {} (status: {})", zipFileName, deleteResponse.getCode());
                                            }
                                        }
                                    }
                                }
                            }
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Could not check for existing assets, proceeding with upload: {}", e.getMessage());
        }
        
        // Upload new asset
        String assetUploadUrl = uploadUrl + "?name=" + zipFileName;
        HttpPost uploadRequest = new HttpPost(assetUploadUrl);
        addAuthHeader(uploadRequest);
        uploadRequest.setHeader("Accept", "application/vnd.github.v3+json");
        uploadRequest.setHeader("Content-Type", "application/zip");
        
        FileEntity entity = new FileEntity(zipFilePath.toFile(), ContentType.APPLICATION_OCTET_STREAM);
        uploadRequest.setEntity(entity);
        
        try (CloseableHttpResponse response = httpClient.execute(uploadRequest)) {
            int statusCode = response.getCode();
            if (statusCode < 200 || statusCode >= 300) {
                try {
                    String body = EntityUtils.toString(response.getEntity());
                    logger.error("Failed to upload asset: {} - {}", statusCode, body);
                } catch (org.apache.hc.core5.http.ParseException pe) {
                    logger.error("Failed to upload asset: {} - (could not parse body)", statusCode);
                }
                return false;
            }
            
            logger.info("Asset uploaded successfully: {}", zipFileName);
            return true;
        }
    }
    
    /**
     * Generates release notes for the release.
     */
    private String generateReleaseNotes(Path zipFilePath, String zipHash, Map<String, FileMetadata> filteredEntries, LocalDate effectiveDate) {
        try {
            long fileSize = Files.size(zipFilePath);
            double fileSizeMB = fileSize / (1024.0 * 1024.0);
            LocalDateTime uploadTime = LocalDateTime.now(ZoneOffset.UTC);
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'");
            String uploadTimeStr = uploadTime.format(timeFormatter);
            String dateStr = effectiveDate.format(DATE_FORMATTER);
            
            StringBuilder notes = new StringBuilder();
            notes.append("## Overview\n\n");
            notes.append("Bundle of **EU Code Lists (EN 16931 artefacts)** explicitly marked with the effective usage date **").append(dateStr).append("**.\n\n");
            notes.append("Includes:\n\n");
            
            // Collect unique artifact types (categories) from entries (excluding JSON/LICENSE/README)
            Set<String> artifactTypes = new LinkedHashSet<>();
            for (FileMetadata entry : filteredEntries.values()) {
                String filename = entry.getDecodedFilename();
                if (filename == null) {
                    filename = entry.getFilename();
                }
                
                // Skip metadata and documentation files
                if (filename != null && (
                    filename.equals("downloaded-files.json") ||
                    filename.equals("LICENSE") ||
                    filename.equals("README-RELEASE.md") ||
                    filename.endsWith("/downloaded-files.json") ||
                    filename.endsWith("/LICENSE") ||
                    filename.endsWith("/README-RELEASE.md")
                )) {
                    continue;
                }
                
                // Get category for display
                String category = getCategoryForDisplay(entry);
                if (category != null && !category.trim().isEmpty()) {
                    artifactTypes.add(category);
                } else {
                    // Fallback: try using the stored category field
                    String storedCategory = entry.getCategory();
                    if (storedCategory != null && !storedCategory.trim().isEmpty()) {
                        // Map stored category to display name
                        String displayCategory = mapStoredCategoryToDisplay(storedCategory);
                        if (displayCategory != null && !displayCategory.trim().isEmpty()) {
                            artifactTypes.add(displayCategory);
                        }
                    } else {
                        logger.warn("Could not determine category for entry: {}", filename);
                    }
                }
            }
            
            // Convert to sorted list for consistent ordering
            List<String> sortedTypes = artifactTypes.stream()
                .sorted()
                .collect(Collectors.toList());
            
            // Number the list
            int count = 1;
            for (String type : sortedTypes) {
                notes.append(count).append(". ").append(type).append("\n\n");
                count++;
            }
            
            // If no types found, log warning
            if (sortedTypes.isEmpty()) {
                logger.warn("No artifact types found for effective date {}, entries count: {}", dateStr, filteredEntries.size());
            }
            
            notes.append("All content originates from the [European Commission registry website](https://ec.europa.eu/digital-building-blocks/sites/spaces/DIGITAL/pages/467108974/Registry+of+supporting+artefacts+to+implement+EN16931).\n\n");
            notes.append("Packaged under [Apache License 2.0](https://github.com/").append(owner).append("/").append(repo).append("?tab=License-1-ov-file#readme), provided as is without warranty.\n\n");
            notes.append("## Package Information\n\n");
            notes.append("**Uploaded:** ").append(uploadTimeStr).append("\n\n");
            notes.append("**Package Size:** ").append(String.format("%,d bytes (%.2f MB)", fileSize, fileSizeMB)).append("\n\n");
            
            if (zipHash != null && !zipHash.isEmpty()) {
                notes.append("**SHA-256 Hash:** ").append(zipHash).append("\n\n");
                notes.append("Use this hash to verify file integrity and detect changes.\n");
            }
            
            return notes.toString();
        } catch (IOException e) {
            logger.error("Error generating release notes", e);
            return "EU Code Lists package of EN 16931 artefacts explicitly marked with effective usage date.";
        }
    }
    
    /**
     * Gets category name for display in the numbered list.
     */
    private String getCategoryForDisplay(FileMetadata entry) {
        // First try CategoryDetector
        String category = null;
        if (entry.getFilename() != null || entry.getUrl() != null) {
            category = CategoryDetector.determineCategoryFromContext(
                entry.getFilename() != null ? entry.getFilename() : "",
                entry.getUrl() != null ? entry.getUrl() : ""
            );
        }
        
        // Handle empty or null categories
        if (category == null || category.trim().isEmpty()) {
            return null;
        }
        
        // Map technical categories to display names
        if ("validation-artefacts-UBL".equals(category)) {
            return "UBL validation artefacts";
        }
        if ("validation-artefacts-CII".equals(category)) {
            return "CII validation artefacts";
        }
        if ("guidance".equals(category)) {
            return "Technical guidance";
        }
        
        // CategoryDetector already returns display-ready names for most categories
        return category;
    }
    
    /**
     * Maps stored category (from FileMetadata.getCategory()) to display name.
     */
    private String mapStoredCategoryToDisplay(String storedCategory) {
        if (storedCategory == null || storedCategory.trim().isEmpty()) {
            return null;
        }
        
        String lower = storedCategory.toLowerCase().trim();
        
        // Map stored category values to display names
        if ("eas".equals(lower)) {
            return "EAS code list";
        }
        if ("vatex-reason-codes".equals(lower) || "vatex".equals(lower)) {
            return "VATEX code list";
        }
        if ("genericodes".equals(lower) || "en 16931 code list - genericode".equals(lower)) {
            return "EN 16931 code list - GeneriCode";
        }
        if ("en16931-code-lists".equals(lower) || "en 16931 code list - xlsx".equals(lower)) {
            return "EN 16931 code list - XLSX";
        }
        if ("validation-artefacts".equals(lower)) {
            // Can't distinguish UBL vs CII from stored category alone
            return "Validation artefacts";
        }
        if ("guidance".equals(lower)) {
            return "Technical guidance";
        }
        
        // If it already looks like a display name (contains spaces or proper capitalization), return as-is
        if (storedCategory.contains(" ") || !storedCategory.equals(lower)) {
            return storedCategory;
        }
        
        return null;
    }
    
    /**
     * Gets human-readable category name for display in release notes.
     */
    private String getHumanReadableCategory(FileMetadata entry) {
        String category = CategoryDetector.determineCategoryFromContext(entry.getFilename(), entry.getUrl());
        
        // Handle empty or null categories
        if (category == null || category.trim().isEmpty()) {
            return "Unknown";
        }
        
        // Convert technical category names to human-readable format
        if ("validation-artefacts-UBL".equals(category)) {
            return "UBL validation artefact";
        }
        if ("validation-artefacts-CII".equals(category)) {
            return "CII validation artefact";
        }
        if ("guidance".equals(category)) {
            return "Technical guidance";
        }
        
        // CategoryDetector already returns human-readable names for most categories
        return category;
    }
    
    /**
     * Gets category name for sorting purposes (handles validation artefacts consistently).
     */
    private String getCategoryForSorting(FileMetadata entry) {
        String category = CategoryDetector.determineCategoryFromContext(entry.getFilename(), entry.getUrl());
        
        // Normalize validation artefacts for consistent sorting
        if ("validation-artefacts-UBL".equals(category) || "validation-artefacts-CII".equals(category)) {
            return "validation-artefacts";
        }
        
        return category != null ? category : "";
    }
    
    /**
     * Calculates SHA-256 hash of a file.
     */
    private String calculateFileHash(Path filePath) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream inputStream = Files.newInputStream(filePath)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    digest.update(buffer, 0, bytesRead);
                }
            }
            
            byte[] hashBytes = digest.digest();
            
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
     * Adds authorization header to the request.
     */
    private void addAuthHeader(ClassicHttpRequest request) {
        if (token != null && !token.isEmpty()) {
            request.setHeader("Authorization", "token " + token);
        }
    }
    
    /**
     * Closes the HTTP client. Should be called when done.
     */
    public void close() throws IOException {
        httpClient.close();
    }
}

