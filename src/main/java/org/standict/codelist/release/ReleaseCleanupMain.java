package org.standict.codelist.release;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utility to delete all releases (and tags) for the configured repository.
 * Usage:
 *   --owner OWNER --repo REPO --github-token TOKEN
 */
public class ReleaseCleanupMain {
    private static final Logger logger = LoggerFactory.getLogger(ReleaseCleanupMain.class);
    private static final String API = "https://api.github.com";

    public static void main(String[] args) throws Exception {
        String owner = null, repo = null, token = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--owner": owner = args[++i]; break;
                case "--repo": repo = args[++i]; break;
                case "--github-token": token = args[++i]; break;
            }
        }
        if (owner == null || repo == null || token == null) {
            System.err.println("Missing required args: --owner --repo --github-token");
            System.exit(1);
        }

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            ObjectMapper mapper = new ObjectMapper();

            // List all releases (paginated)
            int page = 1; int deleted = 0;
            while (true) {
                String listUrl = String.format("%s/repos/%s/%s/releases?per_page=100&page=%d", API, owner, repo, page);
                JsonNode releases = getJson(client, mapper, token, listUrl);
                if (releases == null || !releases.isArray() || releases.size() == 0) break;
                for (JsonNode rel : releases) {
                    long id = rel.get("id").asLong();
                    String tagName = rel.get("tag_name").asText();
                    // Delete release
                    String delUrl = String.format("%s/repos/%s/%s/releases/%d", API, owner, repo, id);
                    if (delete(client, token, delUrl)) {
                        deleted++;
                        logger.info("Deleted release id={} tag={}", id, tagName);
                    }
                    // Delete tag ref (best effort)
                    String delRefUrl = String.format("%s/repos/%s/%s/git/refs/tags/%s", API, owner, repo, tagName);
                    delete(client, token, delRefUrl);
                }
                page++;
            }
            logger.info("Deleted {} releases (tags best-effort)", deleted);
        }
    }

    private static JsonNode getJson(CloseableHttpClient client, ObjectMapper mapper, String token, String url) throws IOException {
        HttpGet req = new HttpGet(url);
        req.setHeader("Accept", "application/vnd.github.v3+json");
        req.setHeader("Authorization", "token " + token);
        try (CloseableHttpResponse resp = client.execute(req)) {
            if (resp.getCode() != 200) return null;
            try {
                String body = EntityUtils.toString(resp.getEntity());
                return mapper.readTree(body);
            } catch (org.apache.hc.core5.http.ParseException pe) {
                return null;
            }
        }
    }

    private static boolean delete(CloseableHttpClient client, String token, String url) throws IOException {
        HttpDelete req = new HttpDelete(url);
        req.setHeader("Accept", "application/vnd.github.v3+json");
        req.setHeader("Authorization", "token " + token);
        try (CloseableHttpResponse resp = client.execute(req)) {
            int code = resp.getCode();
            return code == 204 || code == 200 || code == 202 || code == 404; // 404 ok if tag missing
        }
    }
}


