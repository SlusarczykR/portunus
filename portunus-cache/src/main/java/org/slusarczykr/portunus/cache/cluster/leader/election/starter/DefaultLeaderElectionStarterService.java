package org.slusarczykr.portunus.cache.cluster.leader.election.starter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfig;
import org.slusarczykr.portunus.cache.cluster.leader.exception.PaxosLeaderConflictException;
import org.slusarczykr.portunus.cache.cluster.leader.exception.PaxosLeaderElectionException;
import org.slusarczykr.portunus.cache.cluster.service.AbstractPaxosService;
import org.slusarczykr.portunus.cache.cluster.service.Service;

import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slusarczykr.portunus.cache.cluster.config.ClusterConfig.LeaderElection.DEFAULT_HEARTBEATS_INTERVAL;
import static org.slusarczykr.portunus.cache.cluster.config.ClusterConfig.LeaderElection.DEFAULT_SYNC_STATE_INTERVAL;

public class DefaultLeaderElectionStarterService extends AbstractPaxosService implements LeaderElectionStarterService {

    private static final Logger log = LoggerFactory.getLogger(DefaultLeaderElectionStarterService.class);

    private static final String SEND_HEARTBEATS_JOB = "heartbeats";
    private static final String SYNC_STATE_JOB = "syncState";

    private final ScheduledExecutorService scheduledExecutor;

    private final AtomicReference<CompletableFuture<Boolean>> candidacy = new AtomicReference<>();
    private final Map<String, Future<?>> leaderScheduledJobs = new ConcurrentHashMap<>();

    private final Random random = new Random();

    public static Service newInstance(ClusterService clusterService) {
        return new DefaultLeaderElectionStarterService(clusterService);
    }

    private DefaultLeaderElectionStarterService(ClusterService clusterService) {
        super(clusterService);
        this.scheduledExecutor = createScheduledExecutor();
    }

    private static ScheduledExecutorService createScheduledExecutor() {
        return Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat("leader-election-starter-%d")
                        .build()
        );
    }

    @Override
    public void start() {
        log.debug("Initializing leader election procedure");
        ClusterConfig.LeaderElection leaderElectionConfig = clusterService.getClusterConfig().getLeaderElection();

        if (validateLeaderElectionConfig(leaderElectionConfig)) {
            log.warn("Invalid leader election config. detected! Resetting leader election properties to default values");
            clusterService.getClusterConfig().getLeaderElection().reset();
        }
        startLeaderCandidacy();
    }

    private boolean validateLeaderElectionConfig(ClusterConfig.LeaderElection leaderElectionConfig) {
        return leaderElectionConfig.getHeartbeatsInterval() >= leaderElectionConfig.getMinAwaitTime()
                || leaderElectionConfig.getMinAwaitTime() >= leaderElectionConfig.getMaxAwaitTime();
    }

    private void startLeaderCandidacy() {
        paxosServer.demoteLeader();
        CompletableFuture<Boolean> leaderCandidacy = startLeaderCandidacy(awaitLeaderElectionTime());
        leaderCandidacy.thenAccept(this::processLeaderElection);
        cancelIfPresent(this.candidacy.getAndSet(leaderCandidacy));
    }

    private CompletableFuture<Boolean> startLeaderCandidacy(int timeout) {
        log.debug("Follower will start leader candidacy for {} ms", timeout);
        Executor delayedExecutor = CompletableFuture.delayedExecutor(timeout, MILLISECONDS);
        return CompletableFuture.supplyAsync(this::candidateForLeadership, delayedExecutor);
    }

    private boolean candidateForLeadership() {
        try {
            return clusterService.getLeaderElectionService().startLeaderCandidacy();
        } catch (PaxosLeaderElectionException e) {
            log.error("Could not start leader candidacy", e);
        }
        return false;
    }

    private void processLeaderElection(boolean leader) {
        log.debug("Processing leader election - leader: {}", leader);
        if (Boolean.TRUE.equals(leader)) {
            scheduleHeartbeats();
            scheduleSyncState();
        } else {
            startLeaderCandidacy();
        }
    }


    private void scheduleHeartbeats() {
        int heartbeatsInterval = clusterService.getClusterConfig().getLeaderElection().getHeartbeatsInterval();
        log.debug("Scheduling heartbeats with interval of {}s", heartbeatsInterval);
        ScheduledFuture<?> sendHeartbeatsJob = scheduledExecutor.scheduleAtFixedRate(
                this::sendHeartbeats,
                DEFAULT_HEARTBEATS_INTERVAL,
                heartbeatsInterval,
                SECONDS
        );
        leaderScheduledJobs.put(SEND_HEARTBEATS_JOB, sendHeartbeatsJob);
    }

    private void sendHeartbeats() {
        clusterService.getLeaderElectionService().sendHeartbeats(e -> {
            if (e instanceof PaxosLeaderConflictException) {
                log.error("Leader conflict detected while sending heartbeats to follower nodes!");
                stopLeaderScheduledJobs();
            }
        });
    }

    private void scheduleSyncState() {
        int heartbeatsInterval = clusterService.getClusterConfig().getLeaderElection().getSyncStateInterval();
        log.debug("Scheduling sync server state with interval of {}s", heartbeatsInterval);
        ScheduledFuture<?> syncServerStateJob = scheduledExecutor.scheduleAtFixedRate(
                this::syncServerState,
                DEFAULT_SYNC_STATE_INTERVAL,
                heartbeatsInterval,
                SECONDS
        );
        leaderScheduledJobs.put(SYNC_STATE_JOB, syncServerStateJob);
    }

    private void syncServerState() {
        clusterService.getLeaderElectionService().syncServerState(e -> {
            if (e instanceof PaxosLeaderConflictException) {
                log.error("Leader conflict detected while syncing server state with follower nodes!");
                stopLeaderScheduledJobs();
            }
        });
    }

    @Override
    public void stopLeaderScheduledJobs() {
        log.debug("Stopping cluster leader jobs");
        cancelIfPresent(leaderScheduledJobs.get(SEND_HEARTBEATS_JOB));
        cancelIfPresent(leaderScheduledJobs.get(SYNC_STATE_JOB));
        clusterService.getLeaderElectionStarter().reset();
    }

    private void cancelIfPresent(Future<?> task) {
        Optional.ofNullable(task).ifPresent(it -> {
            log.trace("Canceling current task execution");
            it.cancel(false);
        });
    }

    @Override
    public void reset() {
        log.trace("Resetting leader candidacy starting timeout");
        cancelLeaderCandidacy();
        startLeaderCandidacy();
    }

    private void cancelLeaderCandidacy() {
        cancelIfPresent(candidacy.get());
    }

    @Override
    public boolean stopLeaderScheduledJobsOrReset() {
        log.trace("Getting current leader status");
        boolean leader = paxosServer.isLeader();
        log.trace("Leader status: {}", leader);

        if (leader) {
            clusterService.getLeaderElectionStarter().stopLeaderScheduledJobs();
        }
        clusterService.getLeaderElectionStarter().reset();

        return leader;
    }

    private int awaitLeaderElectionTime() {
        ClusterConfig.LeaderElection leaderElectionConfig = clusterService.getClusterConfig().getLeaderElection();
        return generateRandom(leaderElectionConfig.getMinAwaitTime(), leaderElectionConfig.getMaxAwaitTime()) * 1000;
    }

    private int generateRandom(int min, int max) {
        return random.nextInt(max - min) + min;
    }

    @Override
    public String getName() {
        return LeaderElectionStarterService.class.getSimpleName();
    }

    @Override
    public void shutdown() {
        stopLeaderScheduledJobs();
        cancelLeaderCandidacy();
        scheduledExecutor.shutdown();
    }
}
