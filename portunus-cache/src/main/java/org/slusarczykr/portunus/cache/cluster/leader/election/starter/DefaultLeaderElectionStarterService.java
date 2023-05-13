package org.slusarczykr.portunus.cache.cluster.leader.election.starter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.leader.election.config.LeaderElectionProperties;
import org.slusarczykr.portunus.cache.cluster.leader.exception.PaxosLeaderConflictException;
import org.slusarczykr.portunus.cache.cluster.leader.exception.PaxosLeaderElectionException;
import org.slusarczykr.portunus.cache.cluster.service.AbstractPaxosService;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slusarczykr.portunus.cache.cluster.leader.election.config.LeaderElectionProperties.INITIAL_HEARTBEATS_DELAY;
import static org.slusarczykr.portunus.cache.cluster.leader.election.config.LeaderElectionProperties.INITIAL_SYNC_STATE_DELAY;

public class DefaultLeaderElectionStarterService extends AbstractPaxosService implements LeaderElectionStarterService {

    private static final Logger log = LoggerFactory.getLogger(DefaultLeaderElectionStarterService.class);

    private static final String SEND_HEARTBEATS_JOB = "heartbeats";
    private static final String SYNC_STATE_JOB = "syncState";

    private LeaderElectionProperties leaderElectionProps;
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
    protected void onInitialization() throws PortunusException {
        this.leaderElectionProps = new LeaderElectionProperties();
    }

    @Override
    public void start() {
        log.info("Initializing leader election procedure...");

        if (validateLeaderElectionConfig()) {
            log.warn("Invalid leader election config. detected! Resetting leader election properties to default values...");
            leaderElectionProps.reset();
        }
        startLeaderCandidacy();
    }

    private boolean validateLeaderElectionConfig() {
        return leaderElectionProps.getHeartbeatsInterval() >= leaderElectionProps.getMinAwaitTime()
                || leaderElectionProps.getMinAwaitTime() >= leaderElectionProps.getMaxAwaitTime();
    }

    private void startLeaderCandidacy() {
        paxosServer.demoteLeader();
        CompletableFuture<Boolean> leaderCandidacy = startLeaderCandidacy(awaitLeaderElectionTime());
        leaderCandidacy.thenAccept(this::processLeaderElection);
        cancelIfPresent(this.candidacy.getAndSet(leaderCandidacy));
    }

    private CompletableFuture<Boolean> startLeaderCandidacy(int timeout) {
        log.info("Follower will start leader candidacy for {} ms", timeout);
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
        log.info("Processing leader election - leader: {}", leader);
        if (Boolean.TRUE.equals(leader)) {
            scheduleHeartbeats();
            scheduleSyncState();
        } else {
            startLeaderCandidacy();
        }
    }


    private void scheduleHeartbeats() {
        int heartbeatsInterval = leaderElectionProps.getHeartbeatsInterval();
        log.debug("Scheduling heartbeats with interval of {}s", heartbeatsInterval);
        ScheduledFuture<?> sendHeartbeatsJob = scheduledExecutor.scheduleAtFixedRate(
                this::sendHeartbeats,
                INITIAL_HEARTBEATS_DELAY,
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
        int heartbeatsInterval = leaderElectionProps.getSyncStateInterval();
        log.debug("Scheduling sync server state with interval of {}s", heartbeatsInterval);
        ScheduledFuture<?> syncServerStateJob = scheduledExecutor.scheduleAtFixedRate(
                this::syncServerState,
                INITIAL_SYNC_STATE_DELAY,
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
        cancelIfPresent(leaderScheduledJobs.get(SEND_HEARTBEATS_JOB));
        cancelIfPresent(leaderScheduledJobs.get(SYNC_STATE_JOB));
    }

    private void cancelIfPresent(Future<?> task) {
        Optional.ofNullable(task).ifPresent(it -> {
            log.trace("Canceling current task execution");
            it.cancel(false);
        });
    }

    @Override
    public void reset() {
        log.info("Resetting leader candidacy starting timeout...");
        cancelIfPresent(candidacy.get());
        startLeaderCandidacy();
    }

    private int awaitLeaderElectionTime() {
        return generateRandom(leaderElectionProps.getMinAwaitTime(), leaderElectionProps.getMaxAwaitTime()) * 1000;
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
        scheduledExecutor.shutdown();
    }
}
