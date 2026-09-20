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

import java.net.URLDecoder;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for determining file categories from filenames and URLs.
 * Provides consistent category detection across all phases.
 */
public class CategoryDetector {

    private static final Logger logger = LoggerFactory.getLogger(CategoryDetector.class);
    private static final Set<String> warnedMissingFilenameUrls = ConcurrentHashMap.newKeySet();
    
    /**
     * Determines the category of a file based on its filename and URL context.
     * 
     * @param filename The filename (may contain URL encoding like %20)
     * @param url The full URL of the file
     * @return The category name (e.g., "EAS code list", "EN 16931 code list - GeneriCode")
     */
    public static String determineCategoryFromContext(String filename, String url) {
        String safeUrl = url != null ? url : "";

        if (filename == null) {
            if (warnedMissingFilenameUrls.add(safeUrl)) {
                logger.warn("Category detection received null filename for URL: {}", safeUrl);
            }
            // Best-effort fallback: derive filename from URL path when possible.
            filename = extractFilenameFromUrl(safeUrl);
        }

        // Decode filename for analysis
        String decodedFilename;
        try {
            decodedFilename = URLDecoder.decode(filename, StandardCharsets.UTF_8);
        } catch (Exception e) {
            decodedFilename = filename;
        }
        
        if (decodedFilename == null) {
            decodedFilename = "";
        }
        String lowerFilename = decodedFilename.toLowerCase();
        
        // EAS code list files
        if (lowerFilename.contains("address") && lowerFilename.contains("scheme") && lowerFilename.endsWith(".xlsx")) {
            return "EAS code list";
        }
        
        // VATEX code list files
        if (lowerFilename.contains("exemption") && (lowerFilename.contains("vatex") || lowerFilename.contains("vat exemption")) && lowerFilename.endsWith(".xlsx")) {
            return "VATEX code list";
        }
        
        // EN 16931 code list files - need to distinguish between GeneriCode ZIP and XLSX
        // Check for GeneriCode ZIP files first (they may contain "genericodes" or match patterns like cef-genericodes-*.zip)
        if (lowerFilename.contains("genericodes") && lowerFilename.endsWith(".zip")) {
            return "EN 16931 code list - GeneriCode";
        }
        // Check for EN16931 XLSX files
        if ((lowerFilename.contains("en16931") || lowerFilename.contains("en 16931")) && 
            lowerFilename.contains("code lists") && lowerFilename.endsWith(".xlsx")) {
            return "EN 16931 code list - XLSX";
        }
        // Generic EN16931 ZIP files that are likely GeneriCode (fallback)
        if (lowerFilename.endsWith(".zip") && 
            (lowerFilename.contains("en16931") || lowerFilename.contains("en 16931")) &&
            !lowerFilename.startsWith("en16931-ubl-") && !lowerFilename.startsWith("en16931-cii-")) {
            return "EN 16931 code list - GeneriCode";
        }
        
        // Validation artefacts - split into UBL and CII based on filename
        if (lowerFilename.startsWith("en16931-ubl-") && lowerFilename.endsWith(".zip")) {
            return "validation-artefacts-UBL";
        }
        if (lowerFilename.startsWith("en16931-cii-") && lowerFilename.endsWith(".zip")) {
            return "validation-artefacts-CII";
        }
        
        // Guidance
        if (lowerFilename.contains("technical guidance") && lowerFilename.endsWith(".pdf")) {
            return "guidance";
        }
        
        // Fallback: try to extract from URL
        return extractCategoryFromUrl(safeUrl);
    }

    private static String extractFilenameFromUrl(String url) {
        if (url == null || url.isEmpty()) {
            return "";
        }
        try {
            String path = new URL(url).getPath();
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash >= 0 && lastSlash < path.length() - 1) {
                String name = path.substring(lastSlash + 1);
                int questionMark = name.indexOf('?');
                return questionMark >= 0 ? name.substring(0, questionMark) : name;
            }
        } catch (Exception ignored) {
            // best-effort only
        }
        return "";
    }
    
    /**
     * Extracts category information from URL as a fallback when filename doesn't provide enough context.
     */
    private static String extractCategoryFromUrl(String url) {
        // Look for pattern: .../467108974/[CATEGORY_NAME]...
        // Example: .../467108974/VAT Exemption Reason Code list ......
        Pattern pattern = Pattern.compile("/467108974/([^/]+?)(?:%20|\\s)+(?:Code%20list|version)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            String category = matcher.group(1);
            // Replace %20 with space and clean up
            category = category.replace("%20", " ").trim();
            
            // Map to standardized category names
            String lowerCategory = category.toLowerCase();
            if (lowerCategory.contains("address") && lowerCategory.contains("scheme")) {
                return "EAS code list";
            }
            if (lowerCategory.contains("exemption") && (lowerCategory.contains("vatex") || lowerCategory.contains("vat exemption"))) {
                return "VATEX code list";
            }
            if (lowerCategory.contains("en16931") || lowerCategory.contains("en 16931")) {
                // Can't determine GeneriCode vs XLSX from URL alone, will use filename
                return "EN 16931 code list";
            }
            
            return category;
        }
        return "";
    }
}

