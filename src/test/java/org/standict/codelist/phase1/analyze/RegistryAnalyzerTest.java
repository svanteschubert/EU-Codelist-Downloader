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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.standict.codelist.shared.Configuration;
import org.standict.codelist.shared.FileMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Phase 1 - Registry Analysis with CSV output.
 */
public class RegistryAnalyzerTest {
    
    @Test
    public void testConfigurationDefaults() {
        Configuration config = new Configuration();
        
        assertNotNull(config.getPhase1CsvPath());
        assertTrue(config.getPhase1CsvPath().contains("phase1"));
        assertTrue(config.isWriteLatestCopy());
    }
    
    @Test
    public void testCsvOutputDirectory(@TempDir Path tempDir) throws IOException {
        Configuration config = new Configuration();
        config.setCsvOutputBasePath(tempDir.toString());
        
        String phase1Path = config.getPhase1CsvPath();
        assertTrue(phase1Path.contains("phase1"));
        
        // Verify path would create the directory
        Path phase1Dir = Paths.get(phase1Path);
        Files.createDirectories(phase1Dir);
        assertTrue(Files.exists(phase1Dir));
    }
}

