package org.slusarczykr.portunus.cache.cluster.leader.vote.factory;

import org.slusarczykr.portunus.cache.cluster.leader.api.RequestVote;

public class RequestVoteFactory {

    public RequestVote.Response create(long serverId, long term, RequestVote.Response.Status status) {
        if (RequestVote.Response.Status.ACCEPTED.equals(status)) {
            return new RequestVote.Response.Accepted(serverId, term);
        } else if (RequestVote.Response.Status.REJECTED.equals(status)) {
            return new RequestVote.Response.Rejected(serverId, term);
        }
        return null;
    }
}
