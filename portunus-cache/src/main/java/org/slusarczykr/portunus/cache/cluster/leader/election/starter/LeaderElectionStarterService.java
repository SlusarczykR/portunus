package org.slusarczykr.portunus.cache.cluster.leader.election.starter;

import org.slusarczykr.portunus.cache.cluster.service.Service;

public interface LeaderElectionStarterService extends Service {

    void start();

    void stopLeaderScheduledJobs();

    void reset();

    boolean stopLeaderScheduledJobsOrReset();
}
