package com.netflix.hollow.perf.producer;

import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.Status;
import com.netflix.hollow.api.producer.fs.HollowInMemoryBlobStager;
import com.netflix.hollow.api.producer.listener.IntegrityCheckListener;
import com.netflix.hollow.core.write.objectmapper.HollowHashKey;
import com.netflix.hollow.core.write.objectmapper.HollowInline;
import com.netflix.hollow.core.write.objectmapper.HollowPrimaryKey;
import com.netflix.hollow.core.write.objectmapper.HollowTypeName;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Standalone benchmark for the producer integrity-check phase, using the {@code ocp-live-hierarchy-v1} schema, type
 * sharding, and millions of records. Runs the full producer cycle with {@code parallelPerShardChecksum} off and on and
 * reports the Integrity stage duration for each (readback + the four checksums + delta applies), so the effect of the
 * per-shard parallel checksum on a realistic sharded dataset can be measured.
 *
 * <p>Not a JMH benchmark: each cycle populates millions of records and the integrity phase is seconds long, which does
 * not fit JMH's iteration model. Run via the {@code producerIntegrityBenchmark} Gradle task (which sets a large heap),
 * e.g. {@code ./gradlew :hollow-perf:producerIntegrityBenchmark -Precords=5000000 -PbenchHeap=24g}.
 *
 * <p>Args: {@code [recordCount] [targetMaxTypeShardSize] [mode: false|true|both]}.
 */
public class ProducerIntegrityBenchmark {

    // Bounded pools for the referenced value types so records share (and Hollow dedups) them, keeping the value-type
    // states small and the two top-level record types dominant — as in the real namespace.
    private static final int NUM_STACKS = 5_000;
    private static final int NUM_CACHES = 20_000;
    private static final String[] REGIONS = {"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "sa-east-1", "af-south-1"};
    private static final String[] CAPABILITIES = {"FILL", "SERVE", "PEER", "STAGE", "ORIGIN", "TLS", "IPV6", "HTTP3"};
    private static final String[] RANK_NAMES = {"PRIMARY", "SECONDARY", "TERTIARY", "PEER_ONLY"};
    private static final String[] FILL_LOC_TYPE_NAMES = {"WITHIN_STACK", "OUTSIDE_STACK", "AWS_ORIGIN", "PEER"};

    public static void main(String[] args) throws Exception {
        // Silence Hollow's per-cycle INFO logging so it doesn't drown the benchmark numbers.
        java.util.logging.Logger.getLogger("com.netflix.hollow").setLevel(java.util.logging.Level.WARNING);

        int records = args.length > 0 ? Integer.parseInt(args[0].replace("_", "")) : 5_000_000;
        long shardSize = args.length > 1 ? Long.parseLong(args[1].replace("_", "")) : 32L * 1024 * 1024;
        String mode = args.length > 2 ? args[2] : "both";

        System.out.printf("ProducerIntegrityBenchmark: records=%,d  targetMaxTypeShardSize=%,d  mode=%s  maxHeap=%,dMB%n",
                records, shardSize, mode, Runtime.getRuntime().maxMemory() / (1024 * 1024));

        long off = -1;
        long on = -1;
        if (mode.equals("false") || mode.equals("both")) {
            off = runOnce(records, shardSize, false);
        }
        if (mode.equals("both")) {
            System.gc();
        }
        if (mode.equals("true") || mode.equals("both")) {
            on = runOnce(records, shardSize, true);
        }
        if (off > 0 && on > 0) {
            System.out.printf("%nSUMMARY (warm integrity phase): perType=%,d ms  perShard=%,d ms  speedup=%.2fx%n",
                    off, on, off / (double) on);
        }
    }

    /** Runs a snapshot + two delta cycles and returns the warm (cycle 2) integrity-phase duration in ms. */
    private static long runOnce(int records, long shardSize, boolean parallelPerShard) {
        IntegrityTimer timer = new IntegrityTimer();
        HollowProducer producer = HollowProducer.withPublisher(NO_OP_PUBLISHER)
                .withAnnouncer(NO_OP_ANNOUNCER)
                .withBlobStager(new HollowInMemoryBlobStager())
                .withTargetMaxTypeShardSize(shardSize)
                .withParallelPerShardChecksum(parallelPerShard)
                .build();
        producer.addListener(timer);
        producer.initializeDataModel(StackDownloadableFillHierarchy.class, StackDownloadablePlacement.class);

        long snapStart = System.currentTimeMillis();
        producer.runCycle(ws -> populate(ws, records, 0));
        int shards = producer.getWriteEngine().getTypeState("StackDownloadableFillHierarchy").getNumShards();
        System.out.printf("[parallelPerShard=%-5s] snapshot populated in %,d ms; StackDownloadableFillHierarchy shards=%d%n",
                parallelPerShard, System.currentTimeMillis() - snapStart, shards);

        // Integrity runs only on cycles that have a prior state; run two delta cycles and report both (the second is
        // JIT-warm). The delta is small (a fraction of records changed); the checksum still covers the whole state.
        long warmIntegrityMillis = -1;
        for (int cycle = 1; cycle <= 2; cycle++) {
            final int c = cycle;
            producer.runCycle(ws -> populate(ws, records, c));
            warmIntegrityMillis = timer.lastElapsedMillis;
            System.out.printf("[parallelPerShard=%-5s] integrity phase (delta cycle %d) = %,d ms%n",
                    parallelPerShard, cycle, warmIntegrityMillis);
        }
        return warmIntegrityMillis;
    }

    private static void populate(HollowProducer.WriteState ws, int records, int cycle) {
        for (int i = 0; i < records; i++) {
            StackDownloadableFillHierarchy rec = new StackDownloadableFillHierarchy();
            rec.stackOcAdminId = stackId(i % NUM_STACKS);
            rec.downloadableId = downloadableId(i);
            // Mutate a small fraction on delta cycles so there is a real (but small) delta each cycle.
            rec.packageId = packageId(cycle > 0 && (i % 1000 == 0) ? (long) i + cycle : i);
            rec.outsideStackFillLocations = fillLocations(i, 2);
            rec.withinStackFillLocations = fillLocations(i + 7, 2);
            rec.cacheRoles = cacheRoles(i, 2);
            rec.outsideStackFillLocationGroups = new ArrayList<>(Arrays.asList(fillLocations(i, 2), fillLocations(i + 1, 2)));
            ws.add(rec);

            StackDownloadablePlacement pl = new StackDownloadablePlacement();
            pl.stackOcAdminId = rec.stackOcAdminId;
            pl.downloadableId = rec.downloadableId;
            pl.orderedCurrentCaches = ocaIds(i, 3);
            pl.orderedStagingCaches = ocaIds(i + 1, 2);
            pl.orderedDeprecatedCaches = ocaIds(i + 2, 1);
            pl.orderedPrimaryEligibleAndPeerFillOnlyCaches = ocaIds(i + 3, 2);
            ws.add(pl);
        }
    }

    // ---- deterministic, dedup-friendly builders for the referenced value types ----

    private static StackId stackId(long id) { StackId s = new StackId(); s.ocAdminId = id; return s; }
    private static DownloadableId downloadableId(long id) { DownloadableId d = new DownloadableId(); d.downloadableId = id; return d; }
    private static PackageId packageId(long id) { PackageId p = new PackageId(); p.packageId = id; return p; }
    private static OcaId ocaId(long seed) { OcaId o = new OcaId(); o.ocAdminId = Math.floorMod(seed, NUM_CACHES); return o; }
    private static OriginHopCount hop(int seed) { OriginHopCount h = new OriginHopCount(); h.value = Math.floorMod(seed, 5); return h; }

    private static FillAwsRegion region(int seed) { FillAwsRegion r = new FillAwsRegion(); r._name = REGIONS[Math.floorMod(seed, REGIONS.length)]; return r; }
    private static Capability capability(int seed) { Capability c = new Capability(); c._name = CAPABILITIES[Math.floorMod(seed, CAPABILITIES.length)]; return c; }

    private static FillLocationType fillLocationType(int seed) {
        FillLocationType t = new FillLocationType();
        t._name = FILL_LOC_TYPE_NAMES[Math.floorMod(seed, FILL_LOC_TYPE_NAMES.length)];
        t.isAwsOrigin = "AWS_ORIGIN".equals(t._name);
        return t;
    }

    private static Rank rank(int seed) {
        Rank r = new Rank();
        int s = Math.floorMod(seed, RANK_NAMES.length);
        r._name = RANK_NAMES[s];
        r.tier = s;
        r.isAllowedToFetchFromOutsideStack = (s % 2 == 0);
        r.isAllowedToServeWithinStack = (s < 3);
        return r;
    }

    private static FillLocation fillLocation(int seed) {
        FillLocation f = new FillLocation();
        f.fillLocationType = fillLocationType(seed);
        f.cacheOcAdminId = ocaId(seed);
        f.fillAwsRegion = region(seed);
        f.hopCount = hop(seed);
        return f;
    }

    private static List<FillLocation> fillLocations(int seed, int count) {
        List<FillLocation> list = new ArrayList<>(count);
        for (int j = 0; j < count; j++) {
            list.add(fillLocation(seed + j));
        }
        return list;
    }

    private static List<OcaId> ocaIds(int seed, int count) {
        List<OcaId> list = new ArrayList<>(count);
        for (int j = 0; j < count; j++) {
            list.add(ocaId(seed + j));
        }
        return list;
    }

    private static Set<Capability> capabilities(int seed, int count) {
        Set<Capability> set = new LinkedHashSet<>();
        for (int j = 0; j < count; j++) {
            set.add(capability(seed + j));
        }
        return set;
    }

    private static Map<OcaId, FillRole> cacheRoles(int seed, int count) {
        Map<OcaId, FillRole> map = new LinkedHashMap<>();
        for (int j = 0; j < count; j++) {
            FillRole role = new FillRole();
            role.rank = rank(seed + j);
            role.capabilities = capabilities(seed + j, 2);
            map.put(ocaId(seed + j), role);
        }
        return map;
    }

    // ---- data model (ocp-live-hierarchy-v1 schema) ----

    @HollowTypeName(name = "StackDownloadableFillHierarchy")
    @HollowPrimaryKey(fields = {"stackOcAdminId.ocAdminId", "downloadableId.downloadableId"})
    public static class StackDownloadableFillHierarchy {
        public StackId stackOcAdminId;
        public DownloadableId downloadableId;
        public PackageId packageId;
        public List<FillLocation> outsideStackFillLocations;
        @HollowHashKey(fields = {"ocAdminId"})
        public Map<OcaId, FillRole> cacheRoles;
        public List<FillLocation> withinStackFillLocations;
        public List<List<FillLocation>> outsideStackFillLocationGroups;
    }

    @HollowTypeName(name = "StackDownloadablePlacement")
    @HollowPrimaryKey(fields = {"stackOcAdminId.ocAdminId", "downloadableId.downloadableId"})
    public static class StackDownloadablePlacement {
        public StackId stackOcAdminId;
        public DownloadableId downloadableId;
        public List<OcaId> orderedCurrentCaches;
        public List<OcaId> orderedStagingCaches;
        public List<OcaId> orderedDeprecatedCaches;
        public List<OcaId> orderedPrimaryEligibleAndPeerFillOnlyCaches;
    }

    @HollowTypeName(name = "StackId")
    public static class StackId { public long ocAdminId; }

    @HollowTypeName(name = "DownloadableId")
    public static class DownloadableId { public long downloadableId; }

    @HollowTypeName(name = "PackageId")
    public static class PackageId { public long packageId; }

    @HollowTypeName(name = "OcaId")
    public static class OcaId { public long ocAdminId; }

    @HollowTypeName(name = "OriginHopCount")
    public static class OriginHopCount { public int value; }

    @HollowTypeName(name = "Capability")
    public static class Capability { @HollowInline public String _name; }

    @HollowTypeName(name = "FillAwsRegion")
    public static class FillAwsRegion { @HollowInline public String _name; }

    @HollowTypeName(name = "FillLocationType")
    public static class FillLocationType {
        public boolean isAwsOrigin;
        @HollowInline public String _name;
    }

    @HollowTypeName(name = "Rank")
    public static class Rank {
        public int tier;
        public boolean isAllowedToFetchFromOutsideStack;
        @HollowInline public String _name;
        public boolean isAllowedToServeWithinStack;
    }

    @HollowTypeName(name = "FillLocation")
    public static class FillLocation {
        public FillLocationType fillLocationType;
        public OcaId cacheOcAdminId;
        public FillAwsRegion fillAwsRegion;
        public OriginHopCount hopCount;
    }

    @HollowTypeName(name = "FillRole")
    public static class FillRole {
        public Rank rank;
        @HollowHashKey(fields = {"_name"})
        public Set<Capability> capabilities;
    }

    // ---- infra ----

    private static final class IntegrityTimer implements IntegrityCheckListener {
        volatile long lastElapsedMillis = -1;

        @Override
        public void onIntegrityCheckStart(long version) { }

        @Override
        public void onIntegrityCheckComplete(Status status, HollowProducer.ReadState readState, long version, Duration elapsed) {
            lastElapsedMillis = elapsed.toMillis();
        }
    }

    // No-op publisher: publish(PublishArtifact) MUST be overridden with an empty body — the two default publish
    // methods delegate to each other, so an un-overridden Publisher would recurse infinitely.
    private static final HollowProducer.Publisher NO_OP_PUBLISHER = new HollowProducer.Publisher() {
        @Override
        public void publish(HollowProducer.PublishArtifact artifact) { }
    };

    private static final HollowProducer.Announcer NO_OP_ANNOUNCER = new HollowProducer.Announcer() {
        @Override
        public void announce(long stateVersion) { }
    };
}
