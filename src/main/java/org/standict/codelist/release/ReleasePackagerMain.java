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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Main entry point for packaging releases by effective date.
 * 
 * Usage:
 *   --package-only                    Package ZIPs only (default)
 *   --create-releases                 Also create/update GitHub releases (requires --github-token)
 *   --github-token TOKEN              GitHub token for API access
 *   --owner OWNER                     GitHub repository owner (optional, can be inferred)
 *   --repo REPO                       GitHub repository name (optional, can be inferred)
 */
public class ReleasePackagerMain {
    
    private static final Logger logger = LoggerFactory.getLogger(ReleasePackagerMain.class);
    
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    
    public static void main(String[] args) {
        Map<String, String> arguments = parseArguments(args);
        
        boolean createReleases = arguments.containsKey("--create-releases");
        String githubToken = arguments.get("--github-token");
        String owner = arguments.get("--owner");
        String repo = arguments.get("--repo");
        
        if (createReleases && (githubToken == null || githubToken.isEmpty())) {
            logger.error("--create-releases requires --github-token to be set");
            System.exit(1);
            return;
        }
        
        // Default paths
        String registryFilePath = "src/main/resources/downloaded-files.json";
        String downloadedFilesBaseDir = "src/main/resources/downloaded-files";
        String licensePath = "LICENSE";
        String readmePath = "src/main/resources/README-RELEASE.md";
        String checksumsFilePath = "src/main/resources/releases/checksums.json";
        String outputDir = "target/releases";
        
        try {
            ReleasePackagerMain packager = new ReleasePackagerMain();
            packager.packageReleases(
                registryFilePath,
                downloadedFilesBaseDir,
                licensePath,
                readmePath,
                checksumsFilePath,
                outputDir,
                createReleases,
                githubToken,
                owner,
                repo
            );
        } catch (Exception e) {
            logger.error("Failed to package releases", e);
            System.exit(1);
        }
    }
    
    private static Map<String, String> parseArguments(String[] args) {
        Map<String, String> arguments = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i];
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    arguments.put(key, args[i + 1]);
                    i++; // Skip value
                } else {
                    arguments.put(key, "");
                }
            }
        }
        return arguments;
    }
    
    /**
     * Main packaging logic.
     */
    public void packageReleases(
            String registryFilePath,
            String downloadedFilesBaseDir,
            String licensePath,
            String readmePath,
            String checksumsFilePath,
            String outputDir,
            boolean createReleases,
            String githubToken,
            String owner,
            String repo) throws IOException {
        
        // Initialize components
        EffectiveDateExtractor dateExtractor = new EffectiveDateExtractor();
        FilteredRegistryWriter registryWriter = new FilteredRegistryWriter();
        DeterministicZipper zipper = new DeterministicZipper();
        ChecksumManager checksumManager = new ChecksumManager(checksumsFilePath);
        GitHubReleaseManager githubManager = null;
        
        if (createReleases) {
            // Try to infer owner/repo from git remote if not provided
            if (owner == null || repo == null) {
                String[] gitInfo = inferGitHubInfo();
                if (gitInfo != null) {
                    owner = owner != null ? owner : gitInfo[0];
                    repo = repo != null ? repo : gitInfo[1];
                }
            }
            
            if (owner == null || repo == null) {
                throw new IOException("Cannot determine GitHub owner/repo. Please provide --owner and --repo");
            }
            
            githubManager = new GitHubReleaseManager(owner, repo, githubToken);
            logger.info("GitHub releases enabled for {}/{}", owner, repo);
        }
        
        Path downloadedFilesBase = Paths.get(downloadedFilesBaseDir);
        Path licenseFile = Paths.get(licensePath);
        Path readmeFile = Paths.get(readmePath);
        Path outputBase = Paths.get(outputDir);
        
        // Get all unique effective dates
        Set<LocalDate> effectiveDates = dateExtractor.extractUniqueEffectiveDates(registryFilePath);
        
        logger.info("Processing {} unique effective dates", effectiveDates.size());
        
        int created = 0;
        int skipped = 0;
        int failed = 0;
        
        for (LocalDate effectiveDate : effectiveDates) {
            String dateStr = effectiveDate.format(DATE_FORMATTER);
            logger.info("Processing effective date: {}", dateStr);
            
            try {
                // Filter entries for this date
                Map<String, org.standict.codelist.shared.FileMetadata> filteredEntries = 
                    dateExtractor.filterByEffectiveDate(registryFilePath, effectiveDate);
                
                if (filteredEntries.isEmpty()) {
                    logger.warn("No entries found for effective date {}", dateStr);
                    continue;
                }
                
                // Sanity check the set of artefacts for this date (warnings only, no longer blocking)
                ReleaseSanityChecker sanityChecker = new ReleaseSanityChecker();
                ReleaseSanityChecker.SanityResult sanity = sanityChecker.check(effectiveDate, filteredEntries);
                // Log warnings but continue with packaging
                if (sanity.hasWarnings()) {
                    logger.warn("Packaging {} with warnings (some recommended artefacts may be missing)", dateStr);
                }

                // Create temporary filtered registry JSON
                Path tempRegistryJson = outputBase.resolve("downloaded-files-" + dateStr + ".json");
                Files.createDirectories(tempRegistryJson.getParent());
                registryWriter.writeFilteredRegistry(filteredEntries, tempRegistryJson);
                
                // Create ZIP file path
                String zipFileName = "eu-codelists-" + dateStr + ".zip";
                Path zipFilePath = outputBase.resolve(zipFileName);
                
                // Create ZIP
                zipper.createDeterministicZip(
                    tempRegistryJson,
                    downloadedFilesBase,
                    filteredEntries,
                    licenseFile,
                    readmeFile,
                    zipFilePath
                );
                
                // Calculate and check checksum
                String checksum = checksumManager.calculateFileChecksum(zipFilePath);
                
                // Check if we should skip: only skip if checksum matches AND release exists on GitHub
                boolean shouldSkip = false;
                if (checksumManager.checksumMatches(effectiveDate, zipFilePath)) {
                    if (createReleases && githubManager != null) {
                        // Check if release exists on GitHub
                        try {
                            if (githubManager.releaseExists(effectiveDate)) {
                                // Checksum matches AND release exists - skip
                                shouldSkip = true;
                            } else {
                                // Checksum matches but release doesn't exist - still create it
                                logger.info("Checksum matches but release doesn't exist on GitHub, will create release for {}", dateStr);
                            }
                        } catch (Exception e) {
                            // If we can't check GitHub, assume release doesn't exist and proceed
                            logger.warn("Could not check if release exists on GitHub for {}, proceeding with upload: {}", dateStr, e.getMessage());
                        }
                    } else {
                        // Not creating releases, skip if checksum matches
                        shouldSkip = true;
                    }
                }
                
                if (shouldSkip) {
                    // Checksum matches and release exists (or not creating releases), delete ZIP and skip
                    Files.deleteIfExists(zipFilePath);
                    Files.deleteIfExists(tempRegistryJson);
                    skipped++;
                    logger.info("Skipped {} (unchanged)", zipFileName);
                } else {
                    // Checksum differs or new, or release doesn't exist - keep ZIP and update checksum
                    checksumManager.updateChecksum(effectiveDate, checksum);
                    created++;
                    logger.info("Created {}", zipFileName);
                    
                    // Create/update GitHub release if requested
                    if (createReleases && githubManager != null) {
                        try {
                            boolean success = githubManager.createOrUpdateRelease(
                                effectiveDate,
                                zipFilePath,
                                zipFileName,
                                filteredEntries
                            );
                            if (success) {
                                logger.info("GitHub release created/updated for {}", dateStr);
                            } else {
                                logger.error("Failed to create/update GitHub release for {}", dateStr);
                                failed++;
                            }
                        } catch (Exception e) {
                            logger.error("Error creating GitHub release for {}", dateStr, e);
                            failed++;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to process effective date {}", dateStr, e);
                failed++;
            }
        }
        
        // Save checksums
        checksumManager.saveChecksums();
        
        // Cleanup temporary registry JSON files
        try {
            if (Files.exists(outputBase)) {
                Files.list(outputBase)
                     .filter(p -> p.getFileName().toString().startsWith("downloaded-files-") && 
                                 p.getFileName().toString().endsWith(".json"))
                     .forEach(p -> {
                         try {
                             Files.delete(p);
                         } catch (IOException e) {
                             logger.warn("Could not delete temporary file: {}", p, e);
                         }
                     });
            }
        } catch (IOException e) {
            logger.warn("Could not cleanup temporary files", e);
        }
        
        if (githubManager != null) {
            githubManager.close();
        }
        
        logger.info("Packaging complete: {} created, {} skipped, {} failed", created, skipped, failed);
        
        // Only fail if there were actual processing failures (not warnings)
        if (failed > 0) {
            throw new IOException("Some releases failed to process");
        }
    }
    
    /**
     * Attempts to infer GitHub owner/repo from git remote URL.
     */
    private String[] inferGitHubInfo() {
        try {
            ProcessBuilder pb = new ProcessBuilder("git", "remote", "get-url", "origin");
            Process process = pb.start();
            String output = new String(process.getInputStream().readAllBytes()).trim();
            
            // Parse git URL: git@github.com:owner/repo.git or https://github.com/owner/repo.git
            if (output.contains("github.com")) {
                String[] parts = output.replace(".git", "")
                                      .replace("git@github.com:", "")
                                      .replace("https://github.com/", "")
                                      .replace("http://github.com/", "")
                                      .split("/");
                if (parts.length >= 2) {
                    return new String[]{parts[parts.length - 2], parts[parts.length - 1]};
                }
            }
        } catch (Exception e) {
            logger.debug("Could not infer GitHub info from git remote", e);
        }
        return null;
    }
}

