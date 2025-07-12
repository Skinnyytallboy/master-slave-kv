#include "election.h"

int election_decide_vote(election_state_t *st, int candidate_id, uint64_t req_term, uint64_t req_lsn) {
    if (req_term < st->current_term) {
        return 0;
    }
    if (req_term > st->current_term) {
        st->current_term = req_term;
        /* note: does NOT clear last_vote_term/candidate here - those track
         * "have I voted in term X", and the caller is responsible for
         * demoting to follower on term adoption. that's a role change, not
         * a voting rule, so it lives in server.c, not here. */
    }
    if (st->last_vote_term == req_term) {
        return st->last_vote_candidate == candidate_id;
    }
    if (req_lsn < st->current_lsn) {
        return 0;
    }
    st->last_vote_term = req_term;
    st->last_vote_candidate = candidate_id;
    return 1;
}
