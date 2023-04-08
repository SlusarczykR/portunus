package org.slusarczykr.portunus.cache.cluster.leader.election.starter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.leader.PaxosServer;
import org.slusarczykr.portunus.cache.cluster.leader.election.config.LeaderElectionProperties;
import org.slusarczykr.portunus.cache.cluster.leader.exception.PaxosLeaderConflictException;
import org.slusarczykr.portunus.cache.cluster.leader.exception.PaxosLeaderElectionException;
import org.slusarczykr.portunus.cache.cluster.service.AbstractService;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DefaultLeaderElectionStarterService extends AbstractService implements LeaderElectionStarterService {

    private static final Logger log = LoggerFactory.getLogger(DefaultLeaderElectionStarterService.class);

    private LeaderElectionProperties leaderElectionProps;
    private PaxosServer paxosServer;

    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    private final AtomicReference<CompletableFuture<Boolean>> candidacy = new AtomicReference<>();
    private final AtomicReference<Future<?>> heartbeats = new AtomicReference<>();

    private final Random random = new Random();

    public static Service newInstance(ClusterService clusterService) {
        return new DefaultLeaderElectionStarterService(clusterService);
    }

    private DefaultLeaderElectionStarterService(ClusterService clusterService) {
        super(clusterService);
    }

    @Override
    protected void onInitialization() throws PortunusException {
        this.paxosServer = clusterService.getPortunusClusterInstance().getPaxosServer();
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
            log.error(e.getLocalizedMessage(), e);
        }
        return false;
    }

    private void processLeaderElection(boolean leader) {
        log.info("Processing leader election - leader: {}", leader);
        if (Boolean.TRUE.equals(leader)) {
            scheduleHeartbeats();
        } else {
            startLeaderCandidacy();
        }
    }

    private void scheduleHeartbeats() {
        int heartbeatsInterval = leaderElectionProps.getHeartbeatsInterval();
        log.debug("Scheduling heartbeats with interval of {}s", heartbeatsInterval);
        heartbeats.set(scheduleHeartbeats(heartbeatsInterval));
    }

    private ScheduledFuture<?> scheduleHeartbeats(int interval) {
        return scheduledExecutor.scheduleAtFixedRate(
                this::sendHeartbeats,
                5,
                interval,
                SECONDS
        );
    }

    private void sendHeartbeats() {
        clusterService.getLeaderElectionService().sendHeartbeats(e -> {
            if (e instanceof PaxosLeaderConflictException) {
                log.error("Leader conflict detected while sending heartbeats to followers nodes!");
                stopHeartbeats();
            }
        });
    }

    @Override
    public void stopHeartbeats() {
        cancelIfPresent(heartbeats.get());
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
}
