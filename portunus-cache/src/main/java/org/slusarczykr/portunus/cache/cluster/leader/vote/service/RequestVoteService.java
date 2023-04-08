package org.slusarczykr.portunus.cache.cluster.leader.vote.service;

import org.slusarczykr.portunus.cache.cluster.leader.api.RequestVote;
import org.slusarczykr.portunus.cache.cluster.service.Service;

public interface RequestVoteService extends Service {

    RequestVote.Response vote(RequestVote requestVote);
}
