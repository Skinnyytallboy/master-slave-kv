/* Unit tests for the vote-granting rule used during leader election.
 * Exercises the real production function (src/election.c), feeding it
 * hand-crafted VOTE_REQUESTs the way the manual's Phase 3 tests suggest. */

#include <assert.h>
#include <stdio.h>

#include "election.h"

static int failures = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { printf("FAIL: %s\n", msg); failures++; } \
    else printf("ok: %s\n", msg); \
} while (0)

static election_state_t fresh(uint64_t term, uint64_t lsn) {
    election_state_t st = { term, lsn, 0, -1 };
    return st;
}

static void test_rejects_stale_term(void) {
    election_state_t st = fresh(5, 100);
    int granted = election_decide_vote(&st, 2, 4, 100); /* term 4 < current 5 */
    CHECK(!granted, "vote rejected when candidate's term is behind ours");
    CHECK(st.current_term == 5, "our term is unchanged by a stale request");
}

static void test_grants_first_vote_in_new_term(void) {
    election_state_t st = fresh(5, 100);
    int granted = election_decide_vote(&st, 2, 6, 100);
    CHECK(granted, "vote granted for a brand-new, higher term with an up-to-date log");
    CHECK(st.current_term == 6, "term is adopted alongside the granted vote");
    CHECK(st.last_vote_candidate == 2, "the candidate we voted for is remembered");
}

static void test_rejects_second_candidate_same_term(void) {
    election_state_t st = fresh(5, 100);
    election_decide_vote(&st, 2, 6, 100); /* first vote, granted */
    int granted = election_decide_vote(&st, 3, 6, 100); /* different candidate, same term */
    CHECK(!granted, "a second candidate in the same term is rejected");
}

static void test_regrants_same_candidate_same_term(void) {
    /* a retried VOTE_REQUEST (e.g. after a dropped reply) from the same
     * candidate for the same term should still come back granted */
    election_state_t st = fresh(5, 100);
    election_decide_vote(&st, 2, 6, 100);
    int granted = election_decide_vote(&st, 2, 6, 100);
    CHECK(granted, "a duplicate request from the same already-voted-for candidate is granted again");
}

static void test_rejects_stale_log(void) {
    election_state_t st = fresh(5, 100);
    int granted = election_decide_vote(&st, 2, 6, 50); /* candidate's lsn behind ours */
    CHECK(!granted, "a candidate with a shorter log is rejected even in a new term");
}

static void test_grants_equal_log(void) {
    election_state_t st = fresh(5, 100);
    int granted = election_decide_vote(&st, 2, 6, 100); /* exactly caught up */
    CHECK(granted, "a candidate exactly caught up on lsn is granted a vote");
}

int main(void) {
    test_rejects_stale_term();
    test_grants_first_vote_in_new_term();
    test_rejects_second_candidate_same_term();
    test_regrants_same_candidate_same_term();
    test_rejects_stale_log();
    test_grants_equal_log();

    if (failures == 0) {
        printf("\nall election tests passed\n");
        return 0;
    }
    printf("\n%d election test(s) failed\n", failures);
    return 1;
}
