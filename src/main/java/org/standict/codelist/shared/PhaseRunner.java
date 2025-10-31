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
import org.standict.codelist.phase2.compare.FileComparator;
import org.standict.codelist.phase3.download.FileDownloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;

/**
 * Interactive phase runner for testing individual phases.
 * Allows running each phase separately and inspecting results.
 */
public class PhaseRunner {
    
    private static final Logger logger = LoggerFactory.getLogger(PhaseRunner.class);
    
    public static void main(String[] args) {
        try {
            Configuration config = Configuration.load();
            createDirectories(config);
            
            // If arguments provided, run in non-interactive mode
            if (args.length > 0) {
                String phase = args[0].toLowerCase();
                switch (phase) {
                    case "phase1":
                    case "1":
                        runPhase1(config);
                        break;
                    case "phase2":
                    case "2":
                        runPhase2(config);
                        break;
                    case "phase3":
                    case "3":
                        runPhase3(config);
                        break;
                    case "all":
                    case "4":
                        runAllPhases(config);
                        break;
                    default:
                        System.out.println("Usage: PhaseRunner [phase1|phase2|phase3|all]");
                        System.out.println("Or run without arguments for interactive mode");
                        return;
                }
                return;
            }
            
            // Interactive mode
            Scanner scanner = new Scanner(System.in);
            
            System.out.println("=== Code List Downloader - Phase Runner ===");
            System.out.println("1. Run Phase 1 only (Analysis)");
            System.out.println("2. Run Phase 2 only (Comparison)");
            System.out.println("3. Run Phase 3 only (Download)");
            System.out.println("4. Run all phases sequentially");
            System.out.println("5. Exit");
            System.out.print("Select option: ");
            
            int choice = scanner.nextInt();
            
            switch (choice) {
                case 1:
                    runPhase1(config);
                    break;
                case 2:
                    runPhase2(config);
                    break;
                case 3:
                    runPhase3(config);
                    break;
                case 4:
                    runAllPhases(config);
                    break;
                case 5:
                    System.out.println("Exiting...");
                    break;
                default:
                    System.out.println("Invalid option");
            }
            
        } catch (Exception e) {
            logger.error("Error running phases: {}", e.getMessage(), e);
        }
    }
    
    private static void runPhase1(Configuration config) throws IOException {
        logger.info("=".repeat(60));
        logger.info("Running PHASE 1: Analysis");
        logger.info("=".repeat(60));
        
        RegistryAnalyzer analyzer = new RegistryAnalyzer(config);
        try {
            List<FileMetadata> files = analyzer.analyzeRegistry();
            logger.info("Phase 1 complete. Found {} files.", files.size());
            logger.info("Check results in: {}", config.getPhase1CsvPath());
            
            // Display inventory CSV if it exists
            displayLatestCsv(config.getPhase1CsvPath(), "inventory");
        } finally {
            analyzer.close();
        }
    }
    
    private static void runPhase2(Configuration config) throws IOException {
        logger.info("=".repeat(60));
        logger.info("Running PHASE 2: Comparison");
        logger.info("=".repeat(60));
        
        RegistryAnalyzer analyzer = new RegistryAnalyzer(config);
        FileRegistry registry = new FileRegistry(config.getRegistryFilePath());
        FileComparator comparator = new FileComparator(config, registry);
        
        try {
            // First run Phase 1 to get current files
            List<FileMetadata> currentFiles = analyzer.analyzeRegistry();
            
            // Then run Phase 2 comparison
            List<FileMetadata> filesToDownload = comparator.compareAndDetermineDownloads(currentFiles);
            
            logger.info("Phase 2 complete. {} files need download.", filesToDownload.size());
            logger.info("Check results in: {}", config.getPhase2CsvPath());
            
            displayLatestCsv(config.getPhase2CsvPath(), "diff");
        } finally {
            analyzer.close();
        }
    }
    
    private static void runPhase3(Configuration config) throws IOException, InterruptedException {
        logger.info("=".repeat(60));
        logger.info("Running PHASE 3: Download");
        logger.info("=".repeat(60));
        
        RegistryAnalyzer analyzer = new RegistryAnalyzer(config);
        FileRegistry registry = new FileRegistry(config.getRegistryFilePath());
        FileComparator comparator = new FileComparator(config, registry);
        FileDownloader downloader = new FileDownloader(config, registry);
        
        try {
            // Run Phase 1 and 2 to determine what to download
            List<FileMetadata> currentFiles = analyzer.analyzeRegistry();
            List<FileMetadata> filesToDownload = comparator.compareAndDetermineDownloads(currentFiles);
            
            // Now run Phase 3
            downloader.downloadFiles(filesToDownload, config.isAutoConfirmDownloads());
            
            logger.info("Phase 3 complete.");
            logger.info("Check results in: {}", config.getPhase3CsvPath());
            
            displayLatestCsv(config.getPhase3CsvPath(), "downloads");
        } finally {
            analyzer.close();
            downloader.close();
        }
    }
    
    private static void runAllPhases(Configuration config) throws IOException, InterruptedException {
        logger.info("=".repeat(60));
        logger.info("Running ALL PHASES sequentially");
        logger.info("=".repeat(60));
        
        RegistryAnalyzer analyzer = new RegistryAnalyzer(config);
        FileRegistry registry = new FileRegistry(config.getRegistryFilePath());
        FileComparator comparator = new FileComparator(config, registry);
        FileDownloader downloader = new FileDownloader(config, registry);
        
        try {
            // Phase 1
            logger.info("\n=== PHASE 1: Analysis ===");
            List<FileMetadata> files = analyzer.analyzeRegistry();
            logger.info("Found {} files in registry", files.size());
            displayLatestCsv(config.getPhase1CsvPath(), "inventory");
            
            // Phase 2
            logger.info("\n=== PHASE 2: Comparison ===");
            List<FileMetadata> filesToDownload = comparator.compareAndDetermineDownloads(files);
            logger.info("{} files need download", filesToDownload.size());
            displayLatestCsv(config.getPhase2CsvPath(), "diff");
            
            // Phase 3
            logger.info("\n=== PHASE 3: Download ===");
            downloader.downloadFiles(filesToDownload, config.isAutoConfirmDownloads());
            displayLatestCsv(config.getPhase3CsvPath(), "downloads");
            
            logger.info("\n=== All phases complete! ===");
        } finally {
            analyzer.close();
            downloader.close();
            registry.saveRegistry();
        }
    }
    
    private static void displayLatestCsv(String directory, String prefix) {
        try {
            String latestName = prefix + "-latest.csv";
            Path latestPath = Paths.get(directory, latestName);
            if (Files.exists(latestPath)) {
                logger.info("Contents of {}/{}:", directory, latestName);
                logger.info("-".repeat(60));
                List<String> lines = Files.readAllLines(latestPath);
                for (String line : lines) {
                    logger.info(line);
                }
                logger.info("-".repeat(60));
            } else {
                logger.info("No {} found in {}", latestName, directory);
            }
        } catch (IOException e) {
            logger.warn("Could not read {} in {}: {}", prefix + "-latest.csv", directory, e.getMessage());
        }
    }
    
    private static void createDirectories(Configuration config) throws IOException {
        logger.info("Creating base directories...");
        // Only create base download directory - category directories created on-demand during download
        Files.createDirectories(Paths.get(config.getDownloadBasePath()));
        Files.createDirectories(Paths.get(config.getPhase1CsvPath()));
        Files.createDirectories(Paths.get(config.getPhase2CsvPath()));
        Files.createDirectories(Paths.get(config.getPhase3CsvPath()));
    }
}

