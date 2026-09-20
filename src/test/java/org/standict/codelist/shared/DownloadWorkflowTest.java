package org.standict.codelist.shared;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.standict.codelist.phase2.compare.FileComparator;
import org.standict.codelist.phase3.download.FileDownloader;
import org.standict.codelist.release.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.*;
import java.util.zip.*;

import static org.junit.jupiter.api.Assertions.*;

class DownloadWorkflowTest {
    @TempDir Path directory;
    HttpServer server;
    Configuration config;
    FileRegistry registry;
    String url;
    byte[] payload;
    int status = 200;

    @BeforeEach
    void setup() throws Exception {
        payload = zip("new currency list");
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/digital-genericodes-2026-05-15.zip", exchange -> {
            exchange.sendResponseHeaders(status, payload.length);
            try (var output = exchange.getResponseBody()) { output.write(payload); }
        });
        server.start();
        url = "http://127.0.0.1:" + server.getAddress().getPort() + "/digital-genericodes-2026-05-15.zip";
        config = new Configuration();
        config.setDownloadBasePath(directory.resolve("downloaded-files").toString());
        config.setCsvOutputBasePath(directory.toString());
        config.setDownloadDelaySeconds(0);
        registry = new FileRegistry(config.getRegistryFilePath());
    }

    @AfterEach
    void stop() { server.stop(0); }

    @Test
    void preservesOldRevisionAndPackagesOnlyTheDownloadedReplacement() throws Exception {
        FileMetadata old = existing("?version=1");
        Path originalPath = Path.of(old.getLocalPath());
        byte[] original = Files.readAllBytes(originalPath);
        FileMetadata current = metadata("?version=2");
        List<FileMetadata> pending = new FileComparator(config, registry).compareAndDetermineDownloads(List.of(current));
        assertEquals(1, pending.size());
        registry.saveRegistry();
        // Before a successful replacement download the old revision remains packageable.
        assertEquals(Set.of(old.getUrl()), new EffectiveDateExtractor().filterByEffectiveDate(config.getRegistryFilePath(), old.getEffectiveDate()).keySet());
        try (var download = new DownloaderResource(config, registry)) { download.value.downloadFiles(pending, true); }
        assertNotEquals(originalPath.toString(), old.getLocalPath());
        assertArrayEquals(original, Files.readAllBytes(Path.of(old.getLocalPath())));
        assertArrayEquals(payload, Files.readAllBytes(originalPath));
        assertEquals(old.getFileHash(), FileRegistry.calculateFileHash(old.getLocalPath()));
        assertEquals(current.getUrl(), old.getSupersededBy());

        FileRegistry reloaded = new FileRegistry(config.getRegistryFilePath());
        assertTrue(new FileComparator(config, reloaded).compareAndDetermineDownloads(List.of(metadata("?version=2"))).isEmpty());
        var entries = new EffectiveDateExtractor().filterByEffectiveDate(config.getRegistryFilePath(), current.getEffectiveDate());
        assertEquals(Set.of(current.getUrl()), entries.keySet());
        Path filtered = directory.resolve("filtered.json");
        new FilteredRegistryWriter().writeFilteredRegistry(entries, filtered);
        Path release = directory.resolve("release.zip");
        new DeterministicZipper().createDeterministicZip(filtered, Path.of(config.getDownloadBasePath()), entries,
                Path.of("LICENSE"), Path.of("src/main/resources/README-RELEASE.md"), release);
        try (ZipFile archive = new ZipFile(release.toFile())) {
            String name = "downloaded-files/digital-genericodes-2026-05-15.zip";
            assertArrayEquals(payload, archive.getInputStream(archive.getEntry(name)).readAllBytes());
            assertEquals(1, archive.stream().filter(e -> e.getName().equals(name)).count());
        }
        assertEquals(3, Files.readAllLines(Path.of(config.getDownloadedFilesCsvPath())).size());
    }

    @Test
    void historyDoesNotHideMissingChangedOrCorruptFiles() throws Exception {
        FileMetadata old = existing("?version=1");
        Files.writeString(Path.of(config.getDownloadedFilesCsvPath()), "\"url\"\n\"" + old.getUrl() + "\"\n");
        FileComparator comparator = new FileComparator(config, registry);
        FileMetadata current = metadata("?version=1");
        current.setContentLength(old.getContentLength());
        assertTrue(comparator.compareAndDetermineDownloads(List.of(current)).isEmpty());
        Files.write(Path.of(old.getLocalPath()), new byte[(int)old.getContentLength()]);
        assertEquals(1, comparator.compareAndDetermineDownloads(List.of(current)).size());
        Files.delete(Path.of(old.getLocalPath()));
        assertEquals(1, comparator.compareAndDetermineDownloads(List.of(current)).size());
        Files.write(Path.of(old.getLocalPath()), zip("old currency list"));
        current.setContentLength(old.getContentLength() + 1);
        assertEquals(1, comparator.compareAndDetermineDownloads(List.of(current)).size());
    }

    @Test
    void packagingRejectsCorruptOrMissingRegisteredFiles() throws Exception {
        FileMetadata file = existing("?version=1");
        Path filtered = directory.resolve("filtered.json");
        Files.writeString(filtered, "{}");
        Path local = Path.of(file.getLocalPath());
        Files.writeString(local, "corrupt");
        DeterministicZipper zipper = new DeterministicZipper();
        var entries = Map.of(file.getUrl(), file);
        assertThrows(IOException.class, () -> zipper.createDeterministicZip(filtered,
                Path.of(config.getDownloadBasePath()), entries, Path.of("LICENSE"),
                Path.of("src/main/resources/README-RELEASE.md"), directory.resolve("release.zip")));
        Files.delete(local);
        assertThrows(IOException.class, () -> zipper.createDeterministicZip(filtered,
                Path.of(config.getDownloadBasePath()), entries, Path.of("LICENSE"),
                Path.of("src/main/resources/README-RELEASE.md"), directory.resolve("release.zip")));
    }

    @Test
    void failedHttpResponseDoesNotOverwriteOrRegisterTheReplacement() throws Exception {
        status = 404;
        FileMetadata old = existing("?version=1");
        Path originalPath = Path.of(old.getLocalPath());
        byte[] original = Files.readAllBytes(originalPath);
        FileMetadata replacement = metadata("?version=2");
        try (var download = new DownloaderResource(config, registry)) {
            assertThrows(IOException.class, () -> download.value.downloadFiles(List.of(replacement), true));
        }
        assertArrayEquals(original, Files.readAllBytes(originalPath));
        assertNull(registry.getFile(replacement.getUrl()));
        assertEquals(2, Files.readAllLines(Path.of(config.getDownloadedFilesCsvPath())).size());
        assertEquals(1, Files.readAllLines(Path.of(config.getPhase3CsvPath(), "downloads-latest.csv")).size());
    }

    @Test
    void rejectsHtmlWithSuccessStatusWithoutTouchingAnExistingZip() throws Exception {
        payload = "<html>Temporarily unavailable</html>".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        FileMetadata old = existing("?version=1");
        String hash = old.getFileHash();
        FileMetadata replacement = metadata("?version=2");
        try (var download = new DownloaderResource(config, registry)) {
            assertThrows(IOException.class, () -> download.value.downloadFiles(List.of(replacement), true));
        }
        assertEquals(hash, FileRegistry.calculateFileHash(old.getLocalPath()));
        assertNull(registry.getFile(replacement.getUrl()));
    }

    FileMetadata metadata(String query) {
        FileMetadata file = new FileMetadata(url + query);
        file.setEffectiveDate(LocalDate.of(2026, 5, 15));
        file.setVersion("17");
        file.setLatestRelease(true);
        file.setContentLength(payload.length);
        return file;
    }

    FileMetadata existing(String query) throws Exception {
        FileMetadata file = metadata(query);
        Path local = Path.of(config.getDownloadBasePath(), "EN 16931 code list - GeneriCode", file.getDecodedFilename());
        Files.createDirectories(local.getParent());
        Files.write(local, zip("old currency list"));
        file.setContentLength(Files.size(local));
        file.setActualFileSize(Files.size(local));
        file.setFileHash(FileRegistry.calculateFileHash(local.toString()));
        file.setLocalPath(local.toString());
        file.setDownloaded(true);
        registry.registerFile(file);
        return file;
    }

    static byte[] zip(String text) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream output = new ZipOutputStream(bytes)) {
            ZipEntry entry = new ZipEntry("Currency.gc"); entry.setTime(0);
            output.putNextEntry(entry); output.write(text.getBytes(java.nio.charset.StandardCharsets.UTF_8)); output.closeEntry();
        }
        return bytes.toByteArray();
    }

    // FileDownloader predates AutoCloseable; keep every test's HTTP client closed.
    static class DownloaderResource implements AutoCloseable {
        final FileDownloader value;
        DownloaderResource(Configuration c, FileRegistry r) { value = new FileDownloader(c, r); }
        public void close() { value.close(); }
    }
}
