#ifndef KVDB_ELECTION_H
#define KVDB_ELECTION_H

#include <stdint.h>

typedef struct {
    uint64_t current_term;
    uint64_t current_lsn;
    uint64_t last_vote_term;
    int last_vote_candidate;
} election_state_t;

/* Evaluates a candidate vote request against current term and log freshness. Returns 1 if granted, 0 otherwise. */
int election_decide_vote(election_state_t *st, int candidate_id, uint64_t req_term, uint64_t req_lsn);

#endif
