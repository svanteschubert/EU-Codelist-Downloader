/*
 * Copyright 2025-2026 Svante Schubert
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Main application for downloading and organizing European e-Invoice code lists.
 * 
 * This application monitors the European e-Invoice Technical Advisory Group's
 * registry for new code list files and downloads them into organized directories.
 * 
 * The download process is split into three phases:
 * 1. Analysis phase: Analyzes files without downloading to get metadata (size, last modified, etc.)
 * 2. Comparison phase: Compares with registry to determine NEW/CHANGED/DELETED files
 * 3. Download phase: Downloads only files that are new or have changed
 */
public class CodeListDownloader {
    
    private static final Logger logger = LoggerFactory.getLogger(CodeListDownloader.class);
    
    private final Configuration config;
    private final RegistryAnalyzer analyzer;
    private final FileComparator comparator;
    private final FileDownloader downloader;
    private final FileRegistry fileRegistry;
    private final ScheduledExecutorService scheduler;
    private boolean isScheduledMode = false;
    
    public CodeListDownloader(Configuration config) throws IOException {
        this.config = config;
        this.fileRegistry = new FileRegistry(config.getRegistryFilePath());
        // If registry is empty but Phase 3 CSV exists, seed registry from CSV
        try {
            if (fileRegistry.getAllFiles().isEmpty()) {
                RegistryCsvImporter.seedRegistryFromPhase3Csv(config, fileRegistry);
            }
        } catch (Exception e) {
            logger.warn("Could not seed registry from Phase 3 CSV: {}", e.getMessage());
        }
        this.analyzer = new RegistryAnalyzer(config);
        this.comparator = new FileComparator(config, fileRegistry);
        this.downloader = new FileDownloader(config, fileRegistry);
        this.scheduler = Executors.newScheduledThreadPool(1);
    }
    
    /**
     * Starts the scheduled task to check for new files.
     * 
     * @param initialDelay Initial delay before first check (in seconds)
     * @param period       Interval between checks (in seconds)
     */
    public void start(int initialDelay, int period) {
        logger.info("Starting Code List Downloader in scheduled mode...");
        logger.info("Initial delay: {} seconds, Check interval: {} seconds", initialDelay, period);
        isScheduledMode = true;
        
        scheduler.scheduleAtFixedRate(() -> {
            try {
                checkAndDownloadNewFiles();
            } catch (Exception e) {
                logger.error("Error during scheduled check: {}", e.getMessage(), e);
            }
        }, initialDelay, period, TimeUnit.SECONDS);
        
        logger.info("Code List Downloader started successfully");
    }
    
    /**
     * Checks for new files and downloads them in three phases.
     */
    private void checkAndDownloadNewFiles() {
        logger.info("Starting check for new code list files...");
        try {
            // Phase 1: Analyze files without downloading
            logger.info("=".repeat(60));
            logger.info("Phase 1: Analyzing registry files...");
            logger.info("=".repeat(60));
            List<FileMetadata> availableFiles = analyzer.analyzeRegistry();
            logger.info("Found {} files in registry", availableFiles.size());
            
            // Phase 2: Determine which files need to be downloaded
            logger.info("=".repeat(60));
            logger.info("Phase 2: Comparing with existing downloads...");
            logger.info("=".repeat(60));
            List<FileMetadata> filesToDownload = comparator.compareAndDetermineDownloads(availableFiles);
            
            // Phase 3: Download the files that need updating
            logger.info("=".repeat(60));
            logger.info("Phase 3: Downloading {} files...", filesToDownload.size());
            logger.info("=".repeat(60));
            downloader.downloadFiles(filesToDownload, config.isAutoConfirmDownloads());
            
            logger.info("=".repeat(60));
            logger.info("Check completed successfully");
            logger.info("=".repeat(60));

            // Only log idling message if in scheduled mode
            if (isScheduledMode) {
                int intervalSec = config.getCheckIntervalSeconds();
                java.time.ZonedDateTime nextCheck = java.time.ZonedDateTime.now(java.time.ZoneId.systemDefault())
                    .plusSeconds(intervalSec);
                logger.info(
                    "Idling until next check at {} (in {} seconds). Configure via 'checkIntervalSeconds' in config.json.",
                    nextCheck,
                    intervalSec
                );
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to check and download files", e);
        }
    }
    
    /**
     * Runs a single check/download cycle and stops all services.
     */
    public void runOnce() {
        try {
            checkAndDownloadNewFiles();
        } finally {
            stop();
        }
    }
    
    /**
     * Stops the scheduler and closes all services.
     */
    public void stop() {
        logger.info("Stopping Code List Downloader...");
        scheduler.shutdown();
        analyzer.close();
        downloader.close();
        fileRegistry.saveRegistry();
        logger.info("Code List Downloader stopped");
    }
    
    public static void main(String[] args) {
        try {
            // Load configuration
            Configuration config = Configuration.load();
            if (java.util.Arrays.asList(args).contains("--auto-confirm")) {
                config.setAutoConfirmDownloads(true);
            }
            
            // Create download directories
            createDirectories(config);
            
            // Create and start the downloader
            CodeListDownloader downloader = new CodeListDownloader(config);
            
            // Add shutdown hook for graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received, stopping...");
                downloader.stop();
            }));
            
            // By default, run once and exit (no idling)
            // Use --schedule or --daemon to enable continuous scheduled mode
            boolean scheduleMode = args != null && args.length > 0 && 
                ("--schedule".equalsIgnoreCase(args[0]) || "--daemon".equalsIgnoreCase(args[0]));
            
            if (scheduleMode) {
                // Start the downloader in scheduled mode
                // Run immediately (0 delay) and then at configured interval
                downloader.start(0, config.getCheckIntervalSeconds());
            } else {
                // Run a single cycle and exit (default behavior)
                downloader.runOnce();
                System.exit(0);
            }
            
        } catch (Exception e) {
            logger.error("Application failed: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
    
    /**
     * Creates necessary directories for storing downloaded files and CSV outputs.
     * Note: Download category directories are created on-demand when files are downloaded,
     * so we only create the base path and CSV output directories here.
     */
    private static void createDirectories(Configuration config) throws IOException {
        logger.info("Creating base directories...");
        
        // Only create base download directory - category directories created on-demand during download
        Files.createDirectories(Paths.get(config.getDownloadBasePath()));
        
        // CSV output directories
        Files.createDirectories(Paths.get(config.getPhase1CsvPath()));
        Files.createDirectories(Paths.get(config.getPhase2CsvPath()));
        Files.createDirectories(Paths.get(config.getPhase3CsvPath()));
        
        logger.info("Base directories created successfully");
    }
}
