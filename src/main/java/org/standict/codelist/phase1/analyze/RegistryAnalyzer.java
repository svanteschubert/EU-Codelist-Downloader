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
package org.standict.codelist.phase1.analyze;

import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.ProtocolException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.standict.codelist.shared.Configuration;
import org.standict.codelist.shared.FileMetadata;
import org.standict.codelist.shared.CategoryDetector;
import org.standict.codelist.shared.CsvWriterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.TimeUnit;

/**
 * Phase 1: Analyzes files from the registry without downloading them.
 * Uses HTTP HEAD requests to get metadata like size, last-modified, ETag, etc.
 * Writes inventory CSV to src/main/resources/phase1/
 */
public class RegistryAnalyzer {
    
    private static final Logger logger = LoggerFactory.getLogger(RegistryAnalyzer.class);
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy");
    
    // CSV headers for Phase 1 inventory (quote all fields at write time)
    private static final String[] INVENTORY_HEADERS = {
        "effective_date", "publishing_date", "version", "category", "is_latest_release", "modification_date", "url", "filename", "filetype", "content_length", "content_type", "last_modified"
    };
    
    private final Configuration config;
    private final CloseableHttpClient httpClient;
    
    public RegistryAnalyzer(Configuration config) {
        this.config = config;
        
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
     * Analyzes the registry and returns metadata for all available files.
     * Also writes an inventory CSV file.
     */
    public List<FileMetadata> analyzeRegistry() throws IOException {
        logger.info("Phase 1: Analyzing registry: {}", config.getRegistryUrl());
        
        // Use LinkedHashSet to deduplicate by URL while maintaining insertion order
        Set<FileMetadata> filesSet = new LinkedHashSet<>();
        
        // Fetch the registry page
        HttpGet request = new HttpGet(config.getRegistryUrl());
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            Document doc = Jsoup.parse(
                response.getEntity().getContent(),
                StandardCharsets.UTF_8.name(),
                config.getRegistryUrl()
            );
            
            // First, extract metadata from paragraphs and create a map of URL -> metadata
            Map<String, ParagraphMetadata> urlToMetadata = extractParagraphMetadata(doc);
            logger.info("Extracted metadata for {} URLs from HTML paragraphs", urlToMetadata.size());
            
            // Find all downloadable file links
            Elements links = doc.select("a[href]");
            
            logger.info("Found {} total links", links.size());
            
            int downloadableCount = 0;
            int duplicateCount = 0;
            
            for (Element link : links) {
                String absoluteUrl = link.absUrl("href");
                
                if (isDownloadableFile(absoluteUrl)) {
                    downloadableCount++;
                    try {
                        FileMetadata metadata = analyzeFile(absoluteUrl);
                        if (metadata != null) {
                            // Apply paragraph metadata if available
                            ParagraphMetadata paraMetadata = urlToMetadata.get(absoluteUrl);
                            if (paraMetadata != null) {
                                metadata.setEffectiveDate(paraMetadata.effectiveDate);
                                metadata.setPublishingDate(paraMetadata.publishingDate);
                                metadata.setVersion(paraMetadata.version);
                                metadata.setLatestRelease(paraMetadata.isLatestRelease);
                            }
                            // Do not set publishing date fallback; omit if not provided in HTML
                            
                            // Add to set (deduplicates by URL automatically)
                            if (!filesSet.add(metadata)) {
                                duplicateCount++;
                                logger.debug("Skipped duplicate file: {}", absoluteUrl);
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to analyze file {}: {}", absoluteUrl, e.getMessage());
                    }
                }
            }
            
            logger.info("Found {} downloadable files (code lists, XML, PDFs, etc.), {} duplicates skipped", downloadableCount, duplicateCount);
            
            // Extract publishing dates from all paragraphs and create a map for propagation
            // Do this while doc is still available
            Map<LocalDate, LocalDate> effectiveDateToPublishingDate = extractPublishingDatesFromAllParagraphs(doc);
            
            // Convert Set back to List for the rest of the processing
            List<FileMetadata> files = new ArrayList<>(filesSet);
            
            logger.info("Phase 1 complete. Found {} downloadable files", files.size());
            
            // First ensure effective date and version are filled from filename fallbacks (so XLSX files have versions for propagation)
            ensureReleaseAndVersion(files);
            
            // Propagate publishing dates to files with matching effective dates
            propagatePublishingDates(files, effectiveDateToPublishingDate);
            
            // Then propagate metadata from EN16931 XLSX files to their paired genericode ZIP files
            propagateEn16931Metadata(files);
            
            // Write inventory CSV with enhanced format
            writeInventoryCsv(files);
            
            return files;
        }
    }
    
    /**
     * Inner class to hold metadata extracted from HTML paragraphs.
     */
    private static class ParagraphMetadata {
        LocalDate effectiveDate;
        LocalDate publishingDate;
        String version;
        boolean isLatestRelease;
    }
    
    /**
     * Recursively extracts all text content from an element and all its descendants.
     * This ensures we capture text from nested sub-elements, sub-sub-elements, etc.
     * Also normalizes whitespace to fix dates that are broken by HTML tags like "01<span>/02/21" -> "01/02/21"
     */
    private String getCompleteRecursiveText(Element element) {
        if (element == null) {
            return "";
        }
        // Use Jsoup's text() method which already recursively gets all text content
        // But we'll be explicit about it and also handle potential issues
        String text = element.text();
        // Also try to get text from child elements explicitly to be thorough
        Elements children = element.children();
        if (children.size() > 0 && text.isEmpty()) {
            // If direct text() is empty but has children, gather from children
            StringBuilder sb = new StringBuilder();
            for (Element child : children) {
                String childText = child.text();
                if (!childText.isEmpty()) {
                    if (sb.length() > 0) sb.append(" ");
                    sb.append(childText);
                }
            }
            text = sb.toString();
        }
        // Normalize whitespace: fix dates broken by HTML tags (e.g., "01 /02/21" -> "01/02/21")
        // This handles cases where spans break up date patterns like "01<span>/02/21"
        text = normalizeWhitespaceForDates(text);
        return text;
    }
    
    /**
     * Normalizes whitespace in text, particularly fixing dates that are broken by HTML tags.
     * Examples: "01 /02/21" -> "01/02/21", "15 /03/19" -> "15/03/19"
     */
    private String normalizeWhitespaceForDates(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        // Fix patterns like "01 /02/21" or "15 /03/19" where space breaks the date
        // Pattern: digit(s) + space + "/" + digits -> digit(s) + "/" + digits
        text = text.replaceAll("(\\d{1,2})\\s+(/\\d{2}/\\d{2})", "$1$2");
        // Also fix patterns like "01/ 02/ 21" (spaces after slashes)
        text = text.replaceAll("(\\d{2})/\\s+(\\d{2})/\\s+(\\d{2})", "$1/$2/$3");
        // Normalize multiple spaces to single space
        text = text.replaceAll("\\s+", " ").trim();
        return text;
    }
    
    /**
     * Extracts metadata from HTML paragraphs containing category information.
     * Looks for patterns like "15/05/25 | Published: 04/04/25 | EAS code list – version 14.0"
     */
    private Map<String, ParagraphMetadata> extractParagraphMetadata(Document doc) {
        Map<String, ParagraphMetadata> urlToMetadata = new HashMap<>();
        
        // Find all paragraphs
        Elements paragraphs = doc.select("p");
        
        for (Element para : paragraphs) {
            String paraText = para.text();
            
            // Check if this paragraph contains category information OR "Published:" (to catch metadata even when separated)
            boolean hasCategoryInfo = paraText.contains("EAS code list") || 
                paraText.contains("VATEX code list") || 
                paraText.contains("VAT Exemption Reason Code list") ||
                paraText.contains("EN16931 code list") ||
                paraText.contains("EN16931 code lists") ||
                paraText.contains("Validation artefacts") ||
                paraText.contains("Validation artifacts") ||
                paraText.contains("Genericode files");
            
            boolean hasPublishedDate = paraText.contains("Published:") || paraText.contains("Published: ");
            
            if (hasCategoryInfo || hasPublishedDate) {
                // Combine text from previous sibling if it might contain metadata
                // This handles cases where "Published:" might be in a separate paragraph
                String combinedText = paraText;
                Element prevSibling = para.previousElementSibling();
                if (prevSibling != null && prevSibling.tagName().equals("p")) {
                    String prevText = prevSibling.text();
                    // If previous paragraph has date-like pattern or "Published:", include it
                    if (prevText.matches(".*\\d{2}/\\d{2}/\\d{2}.*") || prevText.contains("Published:") || 
                        prevText.contains("EAS code list") || prevText.contains("VATEX code list") ||
                        prevText.contains("EN16931 code list") || prevText.contains("EN16931 code lists")) {
                        combinedText = prevText + " " + paraText;
                    }
                }
                
                // Also check next sibling for additional context
                Element nextSibling = para.nextElementSibling();
                if (nextSibling != null && nextSibling.tagName().equals("p")) {
                    String nextText = nextSibling.text();
                    if (nextText.contains("Genericode") || nextText.toLowerCase().contains("genericodes") ||
                        nextText.matches(".*\\d{2}/\\d{2}/\\d{2}.*") || nextText.contains("version")) {
                        combinedText = combinedText + " " + nextText;
                    }
                }
                
                // Extract metadata from combined paragraph text
                ParagraphMetadata metadata = parseParagraphMetadata(combinedText);
                if (metadata != null) {
                    // Find all links in this paragraph and associate metadata with them
                    Elements linksInPara = para.select("a[href]");
                    for (Element link : linksInPara) {
                        String absoluteUrl = link.absUrl("href");
                        if (isDownloadableFile(absoluteUrl)) {
                            urlToMetadata.put(absoluteUrl, metadata);
                            logger.debug("Associated metadata (release: {}, pub: {}, version: {}) with URL: {}", 
                                metadata.effectiveDate, metadata.publishingDate, metadata.version, absoluteUrl);
                        }
                    }
                    
                    // Also check immediate next sibling paragraph for links (in case links are in separate para)
                    if (nextSibling != null && nextSibling.tagName().equals("p")) {
                        String nextText = nextSibling.text();
                        if (nextText.contains("Genericode") || nextText.toLowerCase().contains("genericodes") ||
                            nextSibling.select("a[href]").size() > 0) {
                            Elements linksInNext = nextSibling.select("a[href]");
                            for (Element link : linksInNext) {
                                String absoluteUrl = link.absUrl("href");
                                if (isDownloadableFile(absoluteUrl)) {
                                    urlToMetadata.put(absoluteUrl, metadata);
                                    logger.debug("Associated metadata from previous paragraph with URL in next para: {}", absoluteUrl);
                                }
                            }
                        }
                    }
                    
                    // Also check previous sibling for links if current para has "Published:" but no links
                    if (hasPublishedDate && linksInPara.isEmpty() && prevSibling != null && prevSibling.tagName().equals("p")) {
                        Elements linksInPrev = prevSibling.select("a[href]");
                        for (Element link : linksInPrev) {
                            String absoluteUrl = link.absUrl("href");
                            if (isDownloadableFile(absoluteUrl)) {
                                urlToMetadata.put(absoluteUrl, metadata);
                                logger.debug("Associated metadata from paragraph with Published: with URL in previous para: {}", absoluteUrl);
                            }
                        }
                    }
                }
                
                // IMPORTANT: If this paragraph establishes context (hasCategoryInfo), also check following <li> elements
                // The metadata (version, release date, publishing date) might be in list items
                if (hasCategoryInfo) {
                    // Look for <ul> or <ol> that might follow this paragraph
                    Element nextElement = para.nextElementSibling();
                    int siblingCount = 0;
                    while (nextElement != null && siblingCount < 5) {
                        siblingCount++;
                        // Check if it's a list (<ul> or <ol>)
                        if (nextElement.tagName().equals("ul") || nextElement.tagName().equals("ol")) {
                            // Get ALL list items recursively (including nested lists)
                            Elements listItems = nextElement.select("li");
                            for (Element li : listItems) {
                                // Get complete text recursively from all sub and sub-sub elements
                                String liText = getCompleteRecursiveText(li);
                                
                                // Extract metadata from the complete list item text
                                ParagraphMetadata liMetadata = parseParagraphMetadata(liText);
                                if (liMetadata != null || liText.contains("Published:")) {
                                    // If parseParagraphMetadata didn't find it but "Published:" exists, try again
                                    if (liMetadata == null || liMetadata.publishingDate == null) {
                                        liMetadata = parseParagraphMetadata(liText);
                                    }
                                    
                                    // Find ALL links recursively in this list item and ALL descendants
                                    Elements linksInLi = li.select("a[href]");
                                    logger.debug("Found {} links in list item with text: {}", linksInLi.size(), liText.substring(0, Math.min(100, liText.length())));
                                    for (Element link : linksInLi) {
                                        String absoluteUrl = link.absUrl("href");
                                        if (isDownloadableFile(absoluteUrl)) {
                                            if (liMetadata != null) {
                                                urlToMetadata.put(absoluteUrl, liMetadata);
                                                logger.debug("Associated metadata from <li> (release: {}, pub: {}, version: {}) with URL: {}", 
                                                    liMetadata.effectiveDate, liMetadata.publishingDate, liMetadata.version, absoluteUrl);
                                            }
                                        }
                                    }
                                }
                            }
                            // Only process the first list after this paragraph
                            break;
                        }
                        // If next element is not a list, but might contain lists, check for nested lists
                        Elements nestedLists = nextElement.select("ul, ol");
                        for (Element list : nestedLists) {
                            Elements listItems = list.select("li");
                            for (Element li : listItems) {
                                // Get complete text recursively from all sub and sub-sub elements
                                String liText = getCompleteRecursiveText(li);
                                ParagraphMetadata liMetadata = parseParagraphMetadata(liText);
                                if (liMetadata != null || liText.contains("Published:")) {
                                    if (liMetadata == null || liMetadata.publishingDate == null) {
                                        liMetadata = parseParagraphMetadata(liText);
                                    }
                                    // Find ALL links recursively
                                    Elements linksInLi = li.select("a[href]");
                                    logger.debug("Found {} links in nested list item with text: {}", linksInLi.size(), liText.substring(0, Math.min(100, liText.length())));
                                    for (Element link : linksInLi) {
                                        String absoluteUrl = link.absUrl("href");
                                        if (isDownloadableFile(absoluteUrl)) {
                                            if (liMetadata != null) {
                                                urlToMetadata.put(absoluteUrl, liMetadata);
                                                logger.debug("Associated metadata from nested <li> (release: {}, pub: {}, version: {}) with URL: {}", 
                                                    liMetadata.effectiveDate, liMetadata.publishingDate, liMetadata.version, absoluteUrl);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        // Move to next sibling
                        nextElement = nextElement.nextElementSibling();
                    }
                }
            }
        }
        
        // IMPORTANT: Also process ALL list items directly that contain category/metadata information
        // This catches cases where list items have metadata but aren't preceded by a paragraph
        // Handle both flat list items and nested list structures
        Elements allListItems = doc.select("li");
        for (Element li : allListItems) {
            // Get complete text recursively from all sub and sub-sub elements
            String liText = getCompleteRecursiveText(li);
            
            // Check if this list item contains category information or metadata
            // Also check for date+version patterns (like "15/03/19 ... version 1.0") even without explicit category
            boolean hasCategoryInfo = liText.contains("EAS code list") || 
                liText.contains("VATEX code list") || 
                liText.contains("VAT Exemption Reason Code list") ||
                liText.contains("EN16931 code list") ||
                liText.contains("EN16931 code lists") ||
                liText.contains("code lists as used in EN16931") ||
                liText.contains("Validation artefacts") ||
                liText.contains("Validation artifacts") ||
                liText.contains("Genericode files") ||
                liText.contains("Published:");
            
            // Also check if it has a date pattern followed by "version" - this catches entries like:
            // "15/03/19 – Full listing of the code lists as used in EN16931 - version 1.0"
            boolean hasDateAndVersionPattern = Pattern.compile("\\d{2}/\\d{2}/\\d{2}.*?(?:version|Version)", Pattern.CASE_INSENSITIVE).matcher(liText).find();
            
            if (hasCategoryInfo || hasDateAndVersionPattern) {
                // Extract metadata from the complete list item text
                ParagraphMetadata liMetadata = parseParagraphMetadata(liText);
                if (liMetadata != null || liText.contains("Published:")) {
                    // If parseParagraphMetadata didn't find it but "Published:" exists, try again
                    if (liMetadata == null || liMetadata.publishingDate == null) {
                        liMetadata = parseParagraphMetadata(liText);
                    }
                    
                    // Find ALL links recursively in this list item and ALL descendants (including nested lists)
                    Elements linksInLi = li.select("a[href]");
                    if (linksInLi.size() > 0) {
                        logger.debug("Found {} links in list item with category/metadata: {}", linksInLi.size(), liText.substring(0, Math.min(150, liText.length())));
                    }
                    for (Element link : linksInLi) {
                        String absoluteUrl = link.absUrl("href");
                        if (isDownloadableFile(absoluteUrl)) {
                            // Associate metadata even if incomplete - we'll fill gaps later
                            if (liMetadata == null) {
                                liMetadata = new ParagraphMetadata();
                            }
                            // Merge with existing metadata if URL already has some (don't overwrite if existing is better)
                            ParagraphMetadata existing = urlToMetadata.get(absoluteUrl);
                            if (existing == null) {
                                urlToMetadata.put(absoluteUrl, liMetadata);
                                logger.debug("Associated metadata from direct <li> (effective: {}, pub: {}, version: {}) with URL: {}", 
                                    liMetadata.effectiveDate, liMetadata.publishingDate, liMetadata.version, absoluteUrl);
                            } else {
                                // Merge: use non-null values from liMetadata to fill gaps in existing
                                if (existing.effectiveDate == null && liMetadata.effectiveDate != null) {
                                    existing.effectiveDate = liMetadata.effectiveDate;
                                }
                                if (existing.publishingDate == null && liMetadata.publishingDate != null) {
                                    existing.publishingDate = liMetadata.publishingDate;
                                }
                                if ((existing.version == null || existing.version.isEmpty()) && liMetadata.version != null && !liMetadata.version.isEmpty()) {
                                    existing.version = liMetadata.version;
                                }
                                if (!existing.isLatestRelease && liMetadata.isLatestRelease) {
                                    existing.isLatestRelease = true;
                                }
                            }
                        }
                    }
                }
                
                // ALSO: If this list item has nested list items, propagate metadata to nested links
                // This handles cases like:
                // <li>15/11/25 | Published: 23/10/25 (metadata here)
                //   <ul>
                //     <li>UBL link</li>
                //     <li>CII link</li>
                //   </ul>
                // </li>
                Elements nestedLists = li.select("> ul, > ol");  // Direct children only
                for (Element nestedList : nestedLists) {
                    Elements nestedListItems = nestedList.select("li");
                    for (Element nestedLi : nestedListItems) {
                        // Get metadata from parent list item
                        ParagraphMetadata parentMetadata = parseParagraphMetadata(liText);
                        if (parentMetadata != null || liText.contains("Published:")) {
                            if (parentMetadata == null || parentMetadata.publishingDate == null) {
                                parentMetadata = parseParagraphMetadata(liText);
                            }
                            if (parentMetadata == null) {
                                parentMetadata = new ParagraphMetadata();
                            }
                            
                            // Find links in nested list item
                            Elements linksInNestedLi = nestedLi.select("a[href]");
                            for (Element link : linksInNestedLi) {
                                String absoluteUrl = link.absUrl("href");
                                if (isDownloadableFile(absoluteUrl)) {
                                    // Merge with existing metadata
                                    ParagraphMetadata existing = urlToMetadata.get(absoluteUrl);
                                    if (existing == null) {
                                        urlToMetadata.put(absoluteUrl, parentMetadata);
                                        logger.debug("Associated metadata from parent <li> to nested <li> link (release: {}, pub: {}, version: {}) with URL: {}", 
                                            parentMetadata.effectiveDate, parentMetadata.publishingDate, parentMetadata.version, absoluteUrl);
                                    } else {
                                        // Merge metadata
                                        if (existing.effectiveDate == null && parentMetadata.effectiveDate != null) {
                                            existing.effectiveDate = parentMetadata.effectiveDate;
                                        }
                                        if (existing.publishingDate == null && parentMetadata.publishingDate != null) {
                                            existing.publishingDate = parentMetadata.publishingDate;
                                        }
                                        if ((existing.version == null || existing.version.isEmpty()) && parentMetadata.version != null && !parentMetadata.version.isEmpty()) {
                                            existing.version = parentMetadata.version;
                                        }
                                        if (!existing.isLatestRelease && parentMetadata.isLatestRelease) {
                                            existing.isLatestRelease = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return urlToMetadata;
    }
    
    /**
     * Parses metadata from paragraph text like:
     * "15/05/25 | Published: 04/04/25 | EAS code list – version 14.0"
     * or "15/11/25 | Published: 23/10/25 | EAS code list – version 15.0 (latest version)"
     * or "01/02/21 | Published: 01/02/21\n– EN16931 Validation artefacts for UBL 2.1 - version 1.3.4"
     */
    private ParagraphMetadata parseParagraphMetadata(String paraText) {
        ParagraphMetadata metadata = new ParagraphMetadata();
        
        try {
            // Simple extraction: find "Published: " or "Published:" and extract the next 8 characters (dd/mm/yy)
            String lowerParaText = paraText.toLowerCase();
            int publishedIndex = lowerParaText.indexOf("published: ");
            String searchString = "published: ";
            
            // If not found with space, try without space
            if (publishedIndex < 0) {
                publishedIndex = lowerParaText.indexOf("published:");
                searchString = "published:";
            }
            
            if (publishedIndex >= 0) {
                // Extract the 8 characters after "Published:" (skip any whitespace)
                int startIndex = publishedIndex + searchString.length();
                
                // Skip leading whitespace
                while (startIndex < paraText.length() && Character.isWhitespace(paraText.charAt(startIndex))) {
                    startIndex++;
                }
                
                // Extract next 8 characters (dd/mm/yy format)
                if (startIndex + 8 <= paraText.length()) {
                    String publishingDateStr = paraText.substring(startIndex, startIndex + 8);
                    
                    // Validate format (should be dd/mm/yy)
                    if (publishingDateStr.matches("\\d{2}/\\d{2}/\\d{2}")) {
                        metadata.publishingDate = parseDateDDMMYY(publishingDateStr);
                    }
                }
                
                // Extract effective date: find first dd/mm/yy pattern before "Published:"
                // Normalize the text first to handle dates broken by HTML tags
                if (metadata.effectiveDate == null && publishedIndex > 0) {
                    String beforePublished = paraText.substring(0, publishedIndex);
                    beforePublished = normalizeWhitespaceForDates(beforePublished);
                    // Use a more flexible pattern that allows optional spaces: \d{2}\s*/?\s*\d{2}/\d{2}
                    // But also try exact match first
                    Pattern releasePattern = Pattern.compile("(\\d{1,2}\\s*/?\\s*\\d{2}/\\d{2})");
                    Matcher releaseMatcher = releasePattern.matcher(beforePublished);
                    if (releaseMatcher.find()) {
                        String effectiveDateStr = releaseMatcher.group(1);
                        // Normalize the extracted date string
                        effectiveDateStr = normalizeWhitespaceForDates(effectiveDateStr);
                        // Ensure it matches the exact format
                        if (effectiveDateStr.matches("\\d{2}/\\d{2}/\\d{2}")) {
                            LocalDate effectiveDate = parseDateDDMMYY(effectiveDateStr);
                            // Set effective date even if it equals publishing date (they can be the same)
                            if (effectiveDate != null) {
                                metadata.effectiveDate = effectiveDate;
                            }
                        }
                    }
                }
                
                // Extract version number
                // Supports both simple versions (16.0 -> 16) and multi-part versions (1.3.15 -> 1.3.15)
                if (metadata.version == null || metadata.version.isEmpty()) {
                    // Pattern: matches "version 1.3.15", "version 16.0", "version 14", etc.
                    // Use + instead of ? to allow multiple dot-separated parts for validation artefacts
                    Pattern versionPattern = Pattern.compile("(?:version|Version)\\s*(\\d+(?:\\.\\d+)+)", Pattern.CASE_INSENSITIVE);
                    Matcher versionMatcher = versionPattern.matcher(paraText);
                    if (versionMatcher.find()) {
                        String versionStr = versionMatcher.group(1);
                        // Only normalize if it's a simple "X.0" format (e.g., "16.0" -> "16")
                        // Don't normalize multi-part versions like "1.3.15"
                        if (versionStr.matches("^\\d+\\.0$")) {
                            versionStr = versionStr.substring(0, versionStr.length() - 2);
                        }
                        metadata.version = versionStr;
                    } else {
                        // Fallback for simple versions without dots (e.g., "version 14")
                        Pattern simpleVersionPattern = Pattern.compile("(?:version|Version)\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
                        Matcher simpleMatcher = simpleVersionPattern.matcher(paraText);
                        if (simpleMatcher.find()) {
                            metadata.version = simpleMatcher.group(1);
                        }
                    }
                }
                
                metadata.isLatestRelease = paraText.contains("(latest version)");
                
                // Return if we got publishing date (even if release date/version missing - they'll be filled by fallback later)
                if (metadata.publishingDate != null) {
                    return metadata;
                }
            }

            // Pattern 3: Publishing date is missing, but release date and version are present
            // Examples:
            // "16/11/20 | – Full listing of the code lists ... - version 6.0"
            // "17/05/21 | – ... version 7.0"
            // "14/11/18 | EAS code list - version 1.0"
            // "15/03/19 – Full listing of the code lists as used in EN16931 - version 1.0"
            // IMPORTANT: Only match if "Published:" is NOT in the text (otherwise we should have matched above)
            if (publishedIndex < 0 && !lowerParaText.contains("published")) {
                // First try: multi-part versions like 1.3.15 (validation artefacts)
                Pattern pattern3Multi = Pattern.compile(
                    "(\\d{2}/\\d{2}/\\d{2}).*?(?:version|Version)\\s*(\\d+(?:\\.\\d+){2,})",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL
                );
                Matcher matcher3Multi = pattern3Multi.matcher(paraText);
                if (matcher3Multi.find()) {
                    String effectiveDateStr = matcher3Multi.group(1);
                    metadata.effectiveDate = parseDateDDMMYY(effectiveDateStr);
                    String versionStr = matcher3Multi.group(2);
                    metadata.version = versionStr; // Keep multi-part versions as-is
                    metadata.isLatestRelease = paraText.contains("(latest version)");
                    return metadata;
                }
                
                // Second try: versions with dots like "16.0", "1.0" (includes X.0 format)
                Pattern pattern3 = Pattern.compile(
                    "(\\d{2}/\\d{2}/\\d{2}).*?(?:version|Version)\\s*(\\d+(?:\\.\\d+)?)",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL
                );
                Matcher matcher3 = pattern3.matcher(paraText);
                if (matcher3.find()) {
                    String effectiveDateStr = matcher3.group(1);
                    metadata.effectiveDate = parseDateDDMMYY(effectiveDateStr);
                    
                    String versionStr = matcher3.group(2);
                    // Only normalize if it's a simple "X.0" format (e.g., "16.0" -> "16", "1.0" -> "1")
                    // Don't normalize multi-part versions like "1.3.15"
                    if (versionStr.matches("^\\d+\\.0$")) {
                        versionStr = versionStr.substring(0, versionStr.length() - 2);
                    }
                    metadata.version = versionStr;
                    metadata.isLatestRelease = paraText.contains("(latest version)");
                    // publishingDate intentionally left null when not explicitly provided
                    return metadata;
                } else {
                    // Fallback for simple versions without dots (e.g., "version 14")
                    Pattern pattern3Simple = Pattern.compile(
                        "(\\d{2}/\\d{2}/\\d{2}).*?(?:version|Version)\\s*(\\d+)",
                        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
                    );
                    Matcher matcher3Simple = pattern3Simple.matcher(paraText);
                    if (matcher3Simple.find()) {
                        String effectiveDateStr = matcher3Simple.group(1);
                        metadata.effectiveDate = parseDateDDMMYY(effectiveDateStr);
                        metadata.version = matcher3Simple.group(2);
                        metadata.isLatestRelease = paraText.contains("(latest version)");
                        return metadata;
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to parse paragraph metadata from: {}", paraText, e);
        }
        
        return null;
    }
    
    /**
     * Parses date in format dd/mm/yy to LocalDate (dd.MM.yyyy).
     * Assumes years < 50 are 20xx, >= 50 are 19xx.
     */
    private LocalDate parseDateDDMMYY(String dateStr) {
        try {
            String[] parts = dateStr.split("/");
            if (parts.length == 3) {
                int day = Integer.parseInt(parts[0]);
                int month = Integer.parseInt(parts[1]);
                int year = Integer.parseInt(parts[2]);
                
                // Convert 2-digit year to 4-digit
                // Years 00-49 -> 2000-2049, Years 50-99 -> 1950-1999
                if (year < 50) {
                    year += 2000;
                } else {
                    year += 1900;
                }
                
                return LocalDate.of(year, month, day);
            }
        } catch (Exception e) {
            logger.debug("Failed to parse date: {}", dateStr, e);
        }
        return null;
    }
    
    /**
     * Ensures that every file has an effective date and version.
     * If missing, derives them from filename or URL patterns as a fallback.
     */
    private void ensureReleaseAndVersion(List<FileMetadata> files) {
        for (FileMetadata file : files) {
            String decodedFilename = file.getDecodedFilename();
            String lowerFilename = decodedFilename != null ? decodedFilename.toLowerCase() : "";
            String url = file.getUrl() != null ? file.getUrl() : "";

            // Fill version if missing: try from filename
            if (file.getVersion() == null || file.getVersion().isEmpty()) {
                // 1) Validation artefacts: en16931-ubl-1.3.15.zip -> 1.3.15
                Matcher mVal = Pattern.compile("en16931-(?:ubl|cii)-(\\d+(?:\\.\\d+)+)\\.zip").matcher(lowerFilename);
                if (mVal.find()) {
                    file.setVersion(mVal.group(1));
                } else {
                    // 2) Generic version in filename: "version 14.0" or "v14" or "version 1.3.15"
                    // Try multi-part first (for validation artefacts)
                    Matcher mVerMulti = Pattern.compile("(?:version|v)\\s*(\\d+(?:\\.\\d+)+)", Pattern.CASE_INSENSITIVE).matcher(decodedFilename);
                    if (mVerMulti.find()) {
                        String ver = mVerMulti.group(1);
                        // Only normalize if it's a simple "X.0" format
                        if (ver.matches("^\\d+\\.0$")) {
                            ver = ver.substring(0, ver.length() - 2);
                        }
                        file.setVersion(ver);
                    } else {
                        // Fallback for simple versions
                        Matcher mVer = Pattern.compile("(?:version|v)\\s*(\\d+)", Pattern.CASE_INSENSITIVE).matcher(decodedFilename);
                        if (mVer.find()) {
                            file.setVersion(mVer.group(1));
                        }
                    }
                }
            }

            // Fill effective date if missing: try from filename first
            if (file.getEffectiveDate() == null) {
                // 1) YYYY-MM-DD pattern in filename (e.g., cef-genericodes-2024-11-15.zip or ...used from 2021-05-17.xlsx)
                Matcher mDate = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})").matcher(lowerFilename);
                if (mDate.find()) {
                    try {
                        file.setEffectiveDate(LocalDate.parse(mDate.group(1), DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                    } catch (Exception ignored) {}
                }

                // 2) If still missing, try dd/mm/yy in filename text
                if (file.getEffectiveDate() == null) {
                    Matcher mDmy = Pattern.compile("(\\d{2}/\\d{2}/\\d{2})").matcher(decodedFilename);
                    if (mDmy.find()) {
                        LocalDate d = parseDateDDMMYY(mDmy.group(1));
                        if (d != null) file.setEffectiveDate(d);
                    }
                }
                // Do NOT use modificationDate as fallback - only use dates explicitly provided in HTML or filenames
            }
        }
    }
    
    /**
     * Extracts publishing dates from ALL paragraphs in the document.
     * This is a more aggressive scan that looks for any "Published:" date
     * and tries to find its associated effective date, creating a mapping.
     */
    private Map<LocalDate, LocalDate> extractPublishingDatesFromAllParagraphs(Document doc) {
        Map<LocalDate, LocalDate> effectiveDateToPublishingDate = new HashMap<>();
        
        // Check paragraphs
        Elements paragraphs = doc.select("p");
        for (Element para : paragraphs) {
            String paraText = para.text();
            
            // Look for "Published:" or "Published: " anywhere in any paragraph
            if (paraText.contains("Published:") || paraText.contains("Published: ")) {
                // Use parseParagraphMetadata to extract both release date and publishing date
                ParagraphMetadata metadata = parseParagraphMetadata(paraText);
                if (metadata != null && metadata.publishingDate != null) {
                    // If we have both dates, use them
                    if (metadata.effectiveDate != null) {
                        effectiveDateToPublishingDate.put(metadata.effectiveDate, metadata.publishingDate);
                        logger.debug("Extracted publishing date mapping: {} -> {}", metadata.effectiveDate, metadata.publishingDate);
                    } else {
                        // If we only have publishing date, check previous paragraph for effective date
                        Element prevSibling = para.previousElementSibling();
                        if (prevSibling != null && prevSibling.tagName().equals("p")) {
                            String prevText = prevSibling.text();
                            Pattern releasePattern = Pattern.compile("(\\d{2}/\\d{2}/\\d{2})");
                            Matcher prevMatcher = releasePattern.matcher(prevText);
                            if (prevMatcher.find()) {
                                String effectiveDateStr = prevMatcher.group(1);
                                LocalDate effectiveDate = parseDateDDMMYY(effectiveDateStr);
                                if (effectiveDate != null && !effectiveDate.equals(metadata.publishingDate)) {
                                    effectiveDateToPublishingDate.put(effectiveDate, metadata.publishingDate);
                                    logger.debug("Extracted publishing date mapping (from prev para): {} -> {}", effectiveDate, metadata.publishingDate);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Also check ALL <li> elements for publishing dates (this is where metadata often is)
        Elements listItems = doc.select("li");
        for (Element li : listItems) {
            String liText = li.text();
            
            // Look for "Published:" in list items
            if (liText.contains("Published:") || liText.contains("Published: ")) {
                ParagraphMetadata metadata = parseParagraphMetadata(liText);
                if (metadata != null && metadata.publishingDate != null) {
                    if (metadata.effectiveDate != null) {
                        effectiveDateToPublishingDate.put(metadata.effectiveDate, metadata.publishingDate);
                        logger.debug("Extracted publishing date mapping from <li>: {} -> {}", metadata.effectiveDate, metadata.publishingDate);
                    } else {
                        // Try to find effective date in the same list item or previous sibling
                        Pattern releasePattern = Pattern.compile("(\\d{2}/\\d{2}/\\d{2})");
                        Matcher releaseMatcher = releasePattern.matcher(liText);
                        // First check for effective date before "Published:" in the same text
                        if (releaseMatcher.find()) {
                            String effectiveDateStr = releaseMatcher.group(1);
                            LocalDate effectiveDate = parseDateDDMMYY(effectiveDateStr);
                            if (effectiveDate != null && !effectiveDate.equals(metadata.publishingDate)) {
                                effectiveDateToPublishingDate.put(effectiveDate, metadata.publishingDate);
                                logger.debug("Extracted publishing date mapping from <li> (same text): {} -> {}", effectiveDate, metadata.publishingDate);
                            }
                        }
                    }
                }
            }
        }
        
        logger.info("Extracted {} effective date -> publishing date mappings from paragraphs and list items", effectiveDateToPublishingDate.size());
        return effectiveDateToPublishingDate;
    }
    
    /**
     * Propagates publishing dates to files that have matching release dates.
     * If a file has a release date but no publishing date, and we have a mapping
     * for that release date, apply the publishing date.
     */
    private void propagatePublishingDates(List<FileMetadata> files, Map<LocalDate, LocalDate> effectiveDateToPublishingDate) {
        int propagatedCount = 0;
        
        for (FileMetadata file : files) {
            if (file.getEffectiveDate() != null && file.getPublishingDate() == null) {
                LocalDate publishingDate = effectiveDateToPublishingDate.get(file.getEffectiveDate());
                if (publishingDate != null) {
                    file.setPublishingDate(publishingDate);
                    propagatedCount++;
                    logger.debug("Propagated publishing date {} to {} (effective date: {})", 
                        publishingDate, file.getFilename(), file.getEffectiveDate());
                }
            }
        }
        
        if (propagatedCount > 0) {
            logger.info("Propagated publishing dates to {} files based on release date matches", propagatedCount);
        }
    }
    
    /**
     * Propagates metadata from EN16931 XLSX files to their paired genericode ZIP files.
     * For each EN16931 XLSX file that has metadata, finds matching genericode ZIP files
     * (both cef-genericodes-*.zip and old format) and copies the metadata.
     * Matching is based on:
     * 1. Date patterns in filenames matching release dates
     * 2. Version numbers when available in filenames
     */
    private void propagateEn16931Metadata(List<FileMetadata> files) {
        // Build list of EN16931 XLSX files that have metadata
        List<FileMetadata> en16931XlsxWithMetadata = new ArrayList<>();
        
        for (FileMetadata file : files) {
            String decodedFilename = file.getDecodedFilename().toLowerCase();
            
            // Find EN16931 XLSX files that have metadata
            if (decodedFilename.contains("en16931") && 
                decodedFilename.contains("code lists") &&
                decodedFilename.endsWith(".xlsx") &&
                file.getEffectiveDate() != null && 
                file.getVersion() != null) {
                en16931XlsxWithMetadata.add(file);
            }
        }
        
        logger.info("Found {} EN16931 XLSX files with metadata for propagation", en16931XlsxWithMetadata.size());
        int propagatedCount = 0;
        
        // For each EN16931 XLSX with metadata, find and update matching genericode ZIP files
        for (FileMetadata xlsxFile : en16931XlsxWithMetadata) {
            String version = xlsxFile.getVersion();
            LocalDate effectiveDate = xlsxFile.getEffectiveDate();
            LocalDate publishingDate = xlsxFile.getPublishingDate();
            boolean isLatest = xlsxFile.isLatestRelease();
            
            // Extract date from XLSX filename if available (used from YYYY-MM-DD pattern)
            Pattern xlsxDatePattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})");
            Matcher xlsxDateMatcher = xlsxDatePattern.matcher(xlsxFile.getDecodedFilename());
            LocalDate xlsxFilenameDate = null;
            if (xlsxDateMatcher.find()) {
                try {
                    xlsxFilenameDate = LocalDate.parse(xlsxDateMatcher.group(1), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                } catch (Exception e) {
                    // Ignore
                }
            }
            
            // Try to find matching genericode ZIP files
            for (FileMetadata zipFile : files) {
                String zipFilename = zipFile.getDecodedFilename().toLowerCase();
                
                // Skip if already has version (most important - indicates metadata was already propagated)
                // We can still propagate if only effective_date is set but version/publishing_date are missing
                if (zipFile.getVersion() != null && !zipFile.getVersion().isEmpty()) {
                    continue;
                }
                
                // Match old format: "EN16931 code lists values - genericodes -used from YYYY-MM-DD.zip"
                if (zipFilename.contains("en16931") && 
                    zipFilename.contains("genericodes") && 
                    zipFilename.endsWith(".zip")) {
                    
                    // Extract date from filename (YYYY-MM-DD pattern)
                    Pattern datePattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})");
                    Matcher dateMatcher = datePattern.matcher(zipFile.getDecodedFilename());
                    
                    if (dateMatcher.find()) {
                        String dateStr = dateMatcher.group(1);
                        try {
                            LocalDate zipDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                            
                            // Match if dates match (either filename date or release date)
                            boolean datesMatch = false;
                            if (xlsxFilenameDate != null && zipDate.equals(xlsxFilenameDate)) {
                                datesMatch = true;
                            } else if (effectiveDate != null && 
                                (zipDate.equals(effectiveDate) || 
                                 (zipDate.getYear() == effectiveDate.getYear() && 
                                  zipDate.getMonth() == effectiveDate.getMonth()))) {
                                datesMatch = true;
                            }
                            
                            if (datesMatch) {
                                // Copy metadata - always update version and publishing_date if missing
                                if (effectiveDate != null) {
                                    zipFile.setEffectiveDate(effectiveDate);
                                }
                                if (publishingDate != null) {
                                    zipFile.setPublishingDate(publishingDate);
                                }
                                if (version != null && !version.isEmpty()) {
                                    zipFile.setVersion(version);
                                }
                                zipFile.setLatestRelease(isLatest);
                                propagatedCount++;
                                logger.debug("Propagated metadata from {} to {} (effective: {}, pub: {}, version: {})", 
                                    xlsxFile.getFilename(), zipFile.getFilename(), effectiveDate, publishingDate, version);
                            }
                        } catch (Exception e) {
                            // Skip if date parsing fails
                        }
                    }
                }
                
                // Match new format: "cef-genericodes-YYYY-MM-DD.zip" or "digital-genericodes-YYYY-MM-DD.zip"
                if ((zipFilename.startsWith("cef-genericodes-") || 
                     zipFilename.startsWith("digital-genericodes-")) && 
                    zipFilename.endsWith(".zip")) {
                    
                    // Extract date from filename (YYYY-MM-DD pattern)
                    Pattern datePattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})");
                    Matcher dateMatcher = datePattern.matcher(zipFile.getDecodedFilename());
                    
                    if (dateMatcher.find()) {
                        String dateStr = dateMatcher.group(1);
                        try {
                            LocalDate zipDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                            
                            // Match if effective date matches or is close
                            // For genericodes, the date in filename typically represents when they are "used from"
                            // which may match the effective date or be close to it
                            if (effectiveDate != null) {
                                // For genericodes, match if dates are exactly the same (most common case)
                                // or within 7 days (more strict than before to avoid false matches)
                                long daysDiff = Math.abs(java.time.temporal.ChronoUnit.DAYS.between(zipDate, effectiveDate));
                                
                                // Also check if the XLSX filename date matches the genericode ZIP date
                                boolean exactDateMatch = zipDate.equals(effectiveDate);
                                boolean xlsxDateMatch = xlsxFilenameDate != null && zipDate.equals(xlsxFilenameDate);
                                
                                if (exactDateMatch || xlsxDateMatch || daysDiff <= 7) {
                                    logger.info("MATCHING: XLSX {} (v{}) -> ZIP {} (zipDate: {}, effectiveDate: {}, match: {}/{})",
                                        xlsxFile.getDecodedFilename(), version, zipFile.getDecodedFilename(), 
                                        zipDate, effectiveDate, exactDateMatch, xlsxDateMatch);
                                    // Copy metadata - always update version and publishing_date if missing
                                    // Update effective_date only if it's more accurate (from paragraph)
                                    if (effectiveDate != null) {
                                        zipFile.setEffectiveDate(effectiveDate);
                                    }
                                    if (publishingDate != null) {
                                        zipFile.setPublishingDate(publishingDate);
                                    }
                                    if (version != null && !version.isEmpty()) {
                                        zipFile.setVersion(version);
                                    }
                                    zipFile.setLatestRelease(isLatest);
                                    propagatedCount++;
                                    logger.debug("Propagated metadata from {} to {} (effective: {}, pub: {}, version: {})", 
                                        xlsxFile.getFilename(), zipFile.getFilename(), effectiveDate, publishingDate, version);
                                }
                            }
                        } catch (Exception e) {
                            // Skip if date parsing fails
                        }
                    }
                }
            }
        }
        
        if (propagatedCount > 0) {
            logger.info("Propagated metadata to {} genericode ZIP files from EN16931 XLSX files", propagatedCount);
        }
    }
    
    /**
     * Writes the inventory CSV file with enhanced columns.
     */
    private void writeInventoryCsv(List<FileMetadata> files) throws IOException {
        List<String[]> data = new ArrayList<>();
        
        // Sort files by effective date (oldest first), then by category, then by filename (alphabetical)
        List<FileMetadata> sortedFiles = new ArrayList<>(files);
        sortedFiles.sort(new Comparator<FileMetadata>() {
            @Override
            public int compare(FileMetadata f1, FileMetadata f2) {
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
                // We need to determine categories to compare them
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
        
        for (FileMetadata file : sortedFiles) {
            String url = file.getUrl();
            String filename = file.getFilename();
            
            // Extract filetype from filename/URL
            String filetype = extractFiletype(filename);
            
            // Extract modification timestamp from URL and convert to date
            String modificationTimestamp = extractModificationTimestamp(url);
            String modificationDate = convertUnixTimestampToDate(modificationTimestamp);
            
            // Format dates for CSV
            String effectiveDateStr = file.getEffectiveDate() != null ? 
                file.getEffectiveDate().format(DATE_FORMATTER) : "";
            String publishingDateStr = file.getPublishingDate() != null ? 
                file.getPublishingDate().format(DATE_FORMATTER) : "";
            String versionStr = file.getVersion() != null ? file.getVersion() : "";
            String isLatestStr = file.isLatestRelease() ? "true" : "false";
            
            // Determine category based on HTML context and file type
            String category = CategoryDetector.determineCategoryFromContext(filename, url);
            
            data.add(new String[]{
                effectiveDateStr,
                publishingDateStr,
                versionStr,
                category,
                isLatestStr,
                modificationDate,
                url,
                filename,
                filetype,
                String.valueOf(file.getContentLength()),
                file.getContentType() != null ? file.getContentType() : "",
                file.getLastModified() != null ? 
                    file.getLastModified().format(TIMESTAMP_FORMATTER) : ""
            });
        }
        
        Path outputDir = Paths.get(config.getPhase1CsvPath());
        Path csvPath = CsvWriterUtil.writeCsvWithLatestCopy(
            INVENTORY_HEADERS,
            data,
            outputDir,
            "inventory",
            config.isWriteLatestCopy()
        );
        
        // Log completion with all file paths
        logger.info("Phase 1 CSV files written:");
        logger.info("  - Timestamped file: {}", csvPath.toAbsolutePath().normalize());
        if (config.isWriteLatestCopy()) {
            Path latestPath = outputDir.resolve("inventory-latest.csv");
            logger.info("  - Latest copy: {}", latestPath.toAbsolutePath().normalize());
        }
        logger.info("  - Output directory: {}", outputDir.toAbsolutePath().normalize());
    }
    
    private static String extractFiletype(String filename) {
        if (filename == null || filename.isEmpty()) {
            return "";
        }
        int lastDot = filename.lastIndexOf('.');
        if (lastDot > 0 && lastDot < filename.length() - 1) {
            return filename.substring(lastDot + 1).toUpperCase();
        }
        return "";
    }
    
    /**
     * Determines category based on HTML document order and file type.
     * Categories follow the HTML document order:
     * - EAS code list
     * - VATEX code list
     * - EN 16931 code list (with subtypes: GeneriCode for ZIP, XLSX for Excel files)
     */
    private static String extractVersion(String url) {
        // Look for "version" followed by a number or "v" followed by a number
        Pattern pattern = Pattern.compile("(?:version|v)\\s*(\\d+(?:\\.\\d+)?)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }
    
    private static String extractModificationTimestamp(String url) {
        // Extract the modificationDate parameter from the URL
        // Example: ?version=1&modificationDate=1761226169199&api=v2
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
            Instant instant = Instant.ofEpochMilli(timestampMillis);
            return instant.atZone(ZoneOffset.UTC).toLocalDate();
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    private static String convertUnixTimestampToDate(String timestampStr) {
        if (timestampStr == null || timestampStr.isEmpty()) {
            return "";
        }
        try {
            // Parse as milliseconds and convert to Instant
            long timestampMillis = Long.parseLong(timestampStr);
            Instant instant = Instant.ofEpochMilli(timestampMillis);
            
            // Format as ISO 8601 date-time with milliseconds
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
            return instant.atZone(ZoneOffset.UTC).format(formatter);
        } catch (NumberFormatException e) {
            return "";
        }
    }
    
    /**
     * Analyzes a single file using HTTP HEAD request to get metadata without downloading.
     */
    public FileMetadata analyzeFile(String url) throws IOException, ProtocolException {
        logger.debug("Analyzing file metadata: {}", url);
        
        HttpHead request = new HttpHead(url);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            
            int statusCode = response.getCode();
            if (statusCode != 200) {
                logger.warn("File returned status code {}: {}", statusCode, url);
                return null;
            }
            
            FileMetadata metadata = new FileMetadata(url);
            
            // Get content length
            Header contentLengthHeader = response.getHeader(HttpHeaders.CONTENT_LENGTH);
            if (contentLengthHeader != null) {
                try {
                    metadata.setContentLength(Long.parseLong(contentLengthHeader.getValue()));
                } catch (NumberFormatException e) {
                    logger.debug("Could not parse content length for {}: {}", url, e.getMessage());
                }
            }
            
            // Get content type
            Header contentTypeHeader = response.getHeader(HttpHeaders.CONTENT_TYPE);
            if (contentTypeHeader != null) {
                metadata.setContentType(contentTypeHeader.getValue());
            }
            
            // Get last modified
            Header lastModifiedHeader = response.getHeader(HttpHeaders.LAST_MODIFIED);
            if (lastModifiedHeader != null) {
                try {
                    metadata.setLastModified(parseDate(lastModifiedHeader.getValue()));
                } catch (Exception e) {
                    logger.debug("Could not parse last modified for {}: {}", url, e.getMessage());
                }
            }
            
            // Get ETag
            Header etagHeader = response.getHeader(HttpHeaders.ETAG);
            if (etagHeader != null) {
                metadata.setEtag(etagHeader.getValue());
            }
            
            return metadata;
        }
    }
    
    /**
     * Determines if the URL points to a downloadable file (code list, XML, PDF, etc.).
     * Filters out navigation links, documentation pages, and other non-downloadable content.
     */
    private boolean isDownloadableFile(String url) {
        if (url == null || url.isEmpty()) {
            return false;
        }
        
        url = url.toLowerCase();
        
        // Check if URL contains a downloadable file extension
        // Note: query params come after the filename, so we check if extension appears before '?'
        boolean hasDownloadableExtension = url.matches(".*\\.(xlsx|xls|xml|csv|json|zip|pdf)(\\?|$).*");
        
        // Second check: Should be a download/attachment URL
        boolean isDownloadUrl = url.contains("/download/") || 
                               url.contains("/download/attachments/") ||
                               url.contains("/attachments/");
        
        // Third check: Exclude navigation, page links, and trackers
        // But ALLOW attachment downloads even if they have tracker in the path
        boolean isNavigationLink = (!url.contains("attachments")) && (
                                  url.contains("action=") ||
                                  url.contains("breadcrumbs") ||
                                  url.contains("skilink") ||
                                  url.contains("menu") ||
                                  url.contains("hierarchy") ||
                                  url.contains("#header") ||
                                  url.contains("#navigation") ||
                                  url.contains("pageId=") ||
                                  url.contains("display/") ||
                                  url.contains("github.com") ||
                                  url.contains("tracker/plugins"));
        
        // Exclude certain domains/paths (only if NOT an attachment)
        boolean isExcludedDomain = (!url.contains("attachments")) && (
                                   url.contains("atlassian.com") ||
                                   url.contains("confluence.atlassian") ||
                                   url.contains("docs.atlassian") ||
                                   url.contains("support.atlassian"));
        
        // We want files that:
        // 1. Have a downloadable extension AND
        // 2. Are in a download/attachment URL AND
        // 3. Are NOT navigation links AND
        // 4. Are NOT from excluded domains
        return hasDownloadableExtension && 
               isDownloadUrl && 
               !isNavigationLink &&
               !isExcludedDomain;
    }
    
    /**
     * Parses HTTP date format to LocalDateTime.
     */
    private LocalDateTime parseDate(String dateStr) {
        DateTimeFormatter[] formatters = {
            DateTimeFormatter.RFC_1123_DATE_TIME,
            DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        };
        
        for (DateTimeFormatter formatter : formatters) {
            try {
                String cleanDate = dateStr.replaceAll("^\"|\"$", "");
                return LocalDateTime.parse(cleanDate, formatter);
            } catch (Exception e) {
                // Try next formatter
            }
        }
        
        return null;
    }
    
    /**
     * Natural sort comparison (handles embedded numbers correctly).
     * Example: "file1.txt" comes before "file10.txt"
     */
    private int compareNatural(String s1, String s2) {
        int i1 = 0, i2 = 0;
        int len1 = s1.length();
        int len2 = s2.length();
        
        while (i1 < len1 && i2 < len2) {
            char c1 = Character.toLowerCase(s1.charAt(i1));
            char c2 = Character.toLowerCase(s2.charAt(i2));
            
            if (Character.isDigit(c1) && Character.isDigit(c2)) {
                // Both are digits, compare numerically
                int start1 = i1;
                int start2 = i2;
                
                // Read number from s1
                while (i1 < len1 && Character.isDigit(s1.charAt(i1))) i1++;
                // Read number from s2
                while (i2 < len2 && Character.isDigit(s2.charAt(i2))) i2++;
                
                long num1 = Long.parseLong(s1.substring(start1, i1));
                long num2 = Long.parseLong(s2.substring(start2, i2));
                
                if (num1 != num2) {
                    return Long.compare(num1, num2);
                }
            } else {
                // Compare characters
                if (c1 != c2) {
                    return Character.compare(c1, c2);
                }
                i1++;
                i2++;
            }
        }
        
        return Integer.compare(len1 - i1, len2 - i2);
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

