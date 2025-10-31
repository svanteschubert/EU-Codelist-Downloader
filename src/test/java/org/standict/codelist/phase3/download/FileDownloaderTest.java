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
package org.standict.codelist.phase3.download;

import org.junit.jupiter.api.Test;
import org.standict.codelist.shared.Configuration;
import org.standict.codelist.shared.FileRegistry;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Phase 3 - File Download with CSV output.
 */
public class FileDownloaderTest {
    
    @Test
    public void testPhase3CsvPath() {
        Configuration config = new Configuration();
        
        assertNotNull(config.getPhase3CsvPath());
        assertTrue(config.getPhase3CsvPath().contains("phase3"));
    }
    
    @Test
    public void testDownloadDelayConfiguration() {
        Configuration config = new Configuration();
        
        assertTrue(config.getDownloadDelaySeconds() > 0);
        assertEquals(1, config.getDownloadDelaySeconds()); // Default value
    }
    
    @Test
    public void testFileRegistryInitialization() {
        FileRegistry registry = new FileRegistry("test-registry.json");
        
        assertNotNull(registry);
        assertEquals(0, registry.getAllFiles().size());
    }
}

