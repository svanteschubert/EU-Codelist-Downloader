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

