/*
 * Copyright 2025-2026 Svante Schubert
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package org.standict.codelist.shared;

import org.standict.codelist.phase1.analyze.RegistryAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Quick test runner to execute Phase 1 and see results.
 */
public class TestPhases {
    
    private static final Logger logger = LoggerFactory.getLogger(TestPhases.class);
    
    public static void main(String[] args) {
        try {
            Configuration config = Configuration.load();
            createDirectories(config);
            
            logger.info("=".repeat(80));
            logger.info("TESTING PHASE 1: File Detection");
            logger.info("=".repeat(80));
            
            RegistryAnalyzer analyzer = new RegistryAnalyzer(config);
            List<FileMetadata> files = analyzer.analyzeRegistry();
            
            logger.info("\nSUMMARY:");
            logger.info("=".repeat(80));
            logger.info("Total files detected: {}", files.size());
            
            if (!files.isEmpty()) {
                logger.info("\nDetected files:");
                for (FileMetadata file : files) {
                    logger.info("  - {} ({} bytes, type: {})", 
                        file.getFilename(), 
                        file.getContentLength(),
                        file.getCategory());
                }
                
                logger.info("\nCheck CSV output in: {}", config.getPhase1CsvPath());
            }
            
            analyzer.close();
            
        } catch (IOException e) {
            logger.error("Test failed: {}", e.getMessage(), e);
        }
    }
    
    private static void createDirectories(Configuration config) throws IOException {
        // Only create base download directory - category directories created on-demand during download
        Files.createDirectories(Paths.get(config.getDownloadBasePath()));
        Files.createDirectories(Paths.get(config.getPhase1CsvPath()));
        Files.createDirectories(Paths.get(config.getPhase2CsvPath()));
        Files.createDirectories(Paths.get(config.getPhase3CsvPath()));
    }
}

