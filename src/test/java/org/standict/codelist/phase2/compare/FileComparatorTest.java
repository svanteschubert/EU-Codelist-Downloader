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
package org.standict.codelist.phase2.compare;

import org.junit.jupiter.api.Test;
import org.standict.codelist.shared.Configuration;
import org.standict.codelist.shared.FileMetadata;
import org.standict.codelist.shared.FileRegistry;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Phase 2 - File Comparison with CSV output.
 */
public class FileComparatorTest {
    
    @Test
    public void testChangeTypeEnum() {
        FileComparator.ChangeType[] values = FileComparator.ChangeType.values();
        assertEquals(4, values.length);
        assertTrue(List.of(values).contains(FileComparator.ChangeType.NEW));
        assertTrue(List.of(values).contains(FileComparator.ChangeType.CHANGED));
        assertTrue(List.of(values).contains(FileComparator.ChangeType.DELETED));
        assertTrue(List.of(values).contains(FileComparator.ChangeType.UNCHANGED));
    }
    
    @Test
    public void testPhase2CsvPath() {
        Configuration config = new Configuration();
        
        assertNotNull(config.getPhase2CsvPath());
        assertTrue(config.getPhase2CsvPath().contains("phase2"));
    }
    
    @Test
    public void testFileMetadataCreation() {
        FileMetadata metadata = new FileMetadata("https://example.com/test.xml");
        
        assertNotNull(metadata);
        assertEquals("https://example.com/test.xml", metadata.getUrl());
        assertNotNull(metadata.getFilename());
        assertNotNull(metadata.getCategory());
    }
}

