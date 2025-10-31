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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Extracts all hyperlinks from the registry page and saves them to a file.
 * Useful for detecting new links that have been added to the registry.
 */
public class RegistryLinkExtractor {
    
    private static final Logger logger = LoggerFactory.getLogger(RegistryLinkExtractor.class);
    
    private final Configuration config;
    
    public RegistryLinkExtractor(Configuration config) {
        this.config = config;
    }
    
    /**
     * Extracts all hyperlinks from the registry page.
     */
    public List<String> extractAllLinks() throws IOException {
        logger.info("Fetching registry page: {}", config.getRegistryUrl());
        
        Document doc = Jsoup.connect(config.getRegistryUrl())
                .timeout(30000)
                .get();
        
        // Find all links
        Elements links = doc.select("a[href]");
        
        Set<String> uniqueLinks = new HashSet<>();
        List<String> allLinks = new ArrayList<>();
        
        for (Element link : links) {
            String absHref = link.absUrl("href");
            String text = link.text();
            
            if (absHref != null && !absHref.isEmpty()) {
                uniqueLinks.add(absHref);
                allLinks.add(String.format("%s | %s", absHref, text));
            }
        }
        
        logger.info("Found {} total links, {} unique links", allLinks.size(), uniqueLinks.size());
        
        return allLinks;
    }
    
    /**
     * Saves links to a file.
     */
    public void saveLinksToFile(List<String> links, String filename) throws IOException {
        Path outputDir = Paths.get("src/main/resources");
        Files.createDirectories(outputDir);
        
        Path outputPath = outputDir.resolve(filename);
        
        List<String> lines = new ArrayList<>();
        lines.add("Registry Links - " + java.time.LocalDateTime.now());
        lines.add("=".repeat(80));
        lines.add("");
        
        for (String link : links) {
            lines.add(link);
        }
        
        Files.write(outputPath, lines, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        logger.info("Saved {} links to {}", links.size(), outputPath);
    }
    
    /**
     * Groups links by type/category for better analysis.
     */
    public void analyzeAndSaveLinks() throws IOException {
        logger.info("Analyzing links from registry...");
        
        Document doc = Jsoup.connect(config.getRegistryUrl())
                .timeout(30000)
                .get();
        
        Elements links = doc.select("a[href]");
        
        List<String> easLinks = new ArrayList<>();
        List<String> en16931Links = new ArrayList<>();
        List<String> vatexLinks = new ArrayList<>();
        List<String> validationArtefactLinks = new ArrayList<>();
        List<String> genericodesLinks = new ArrayList<>();
        List<String> guidanceLinks = new ArrayList<>();
        List<String> otherLinks = new ArrayList<>();
        List<String> nonDownloadableLinks = new ArrayList<>();
        
        for (Element link : links) {
            String absHref = link.absUrl("href");
            String text = link.text();
            
            if (absHref == null || absHref.isEmpty()) {
                continue;
            }
            
            String lowerHref = absHref.toLowerCase();
            
            // Check for EAS (Electronic Address Scheme)
            if (lowerHref.contains("address") && lowerHref.contains("scheme")) {
                easLinks.add(String.format("%s | %s", absHref, text));
            }
            // Check for EN16931 code lists
            else if (lowerHref.contains("en16931") && lowerHref.contains("code")) {
                en16931Links.add(String.format("%s | %s", absHref, text));
            }
            // Check for VATEX (VAT Exemption)
            else if (lowerHref.contains("exemption") || lowerHref.contains("vatex")) {
                vatexLinks.add(String.format("%s | %s", absHref, text));
            }
            // Check for validation artefacts (UBL/CII)
            else if (lowerHref.contains("en16931-ubl") || lowerHref.contains("en16931-cii")) {
                validationArtefactLinks.add(String.format("%s | %s", absHref, text));
            }
            // Check for genericodes
            else if (lowerHref.contains("genericodes")) {
                genericodesLinks.add(String.format("%s | %s", absHref, text));
            }
            // Check for guidance documents
            else if (lowerHref.contains("guidance") || lowerHref.contains("technical") || 
                     lowerHref.endsWith(".pdf")) {
                guidanceLinks.add(String.format("%s | %s", absHref, text));
            }
            // Other downloadable files (if any don't match above categories)
            else if (lowerHref.endsWith(".xlsx") || lowerHref.endsWith(".zip") || 
                     lowerHref.endsWith(".xml") || lowerHref.endsWith(".pdf") ||
                     lowerHref.endsWith(".xls") || lowerHref.endsWith(".csv")) {
                otherLinks.add(String.format("%s | %s", absHref, text));
            }
            // Non-downloadable links (navigation, internal links, etc.)
            else {
                nonDownloadableLinks.add(String.format("%s | %s", absHref, text));
            }
        }
        
        logger.info("EAS (Electronic Address Scheme) links: {}", easLinks.size());
        logger.info("EN16931 Code Lists links: {}", en16931Links.size());
        logger.info("VATEX (VAT Exemption) links: {}", vatexLinks.size());
        logger.info("Validation Artefacts (UBL/CII) links: {}", validationArtefactLinks.size());
        logger.info("Genericodes links: {}", genericodesLinks.size());
        logger.info("Guidance links: {}", guidanceLinks.size());
        logger.info("Other downloadable links: {}", otherLinks.size());
        logger.info("Non-downloadable links: {}", nonDownloadableLinks.size());
        
        // Save categorized links
        saveCategorizedLinks(easLinks, en16931Links, vatexLinks, validationArtefactLinks, 
                           genericodesLinks, guidanceLinks, otherLinks, nonDownloadableLinks);
    }
    
    private void saveCategorizedLinks(List<String> easLinks, 
                                     List<String> en16931Links,
                                     List<String> vatexLinks,
                                     List<String> validationArtefactLinks,
                                     List<String> genericodesLinks,
                                     List<String> guidanceLinks,
                                     List<String> otherLinks,
                                     List<String> nonDownloadableLinks) throws IOException {
        Path outputDir = Paths.get("src/main/resources");
        Files.createDirectories(outputDir);
        
        List<String> lines = new ArrayList<>();
        lines.add("Registry Links Analysis - " + java.time.LocalDateTime.now());
        lines.add("=".repeat(80));
        lines.add("");
        
        lines.add("=== EAS (ELECTRONIC ADDRESS SCHEME) LINKS ===");
        lines.add("");
        easLinks.forEach(lines::add);
        lines.add("");
        
        lines.add("=== EN16931 CODE LISTS LINKS ===");
        lines.add("");
        en16931Links.forEach(lines::add);
        lines.add("");
        
        lines.add("=== VATEX (VAT EXEMPTION) LINKS ===");
        lines.add("");
        vatexLinks.forEach(lines::add);
        lines.add("");
        
        lines.add("=== VALIDATION ARTEFACTS (UBL/CII) LINKS ===");
        lines.add("");
        validationArtefactLinks.forEach(lines::add);
        lines.add("");
        
        lines.add("=== GENERICODES LINKS ===");
        lines.add("");
        genericodesLinks.forEach(lines::add);
        lines.add("");
        
        lines.add("=== GUIDANCE DOCUMENTS LINKS ===");
        lines.add("");
        guidanceLinks.forEach(lines::add);
        lines.add("");
        
        lines.add("=== OTHER DOWNLOADABLE LINKS ===");
        lines.add("");
        otherLinks.forEach(lines::add);
        lines.add("");
        
        lines.add("=== NON-DOWNLOADABLE LINKS (Navigation, etc.) ===");
        lines.add("");
        if (nonDownloadableLinks.isEmpty()) {
            lines.add("(none - all links are downloadable resources)");
        } else {
            nonDownloadableLinks.forEach(lines::add);
        }
        
        Path outputPath = outputDir.resolve("registry-links-analysis.txt");
        Files.write(outputPath, lines, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        
        logger.info("Saved categorized links to {}", outputPath);
    }
    
    public static void main(String[] args) {
        try {
            Configuration config = Configuration.load();
            RegistryLinkExtractor extractor = new RegistryLinkExtractor(config);
            
            logger.info("Extracting all links from registry...");
            extractor.analyzeAndSaveLinks();
            
            logger.info("Link extraction complete. Check src/main/resources/registry-links-analysis.txt");
            
        } catch (IOException e) {
            logger.error("Failed to extract links: {}", e.getMessage(), e);
        }
    }
}

