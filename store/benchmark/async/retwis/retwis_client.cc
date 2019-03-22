#include "store/benchmark/async/retwis/retwis_client.h"

#include "store/benchmark/async/retwis/add_user.h"
#include "store/benchmark/async/retwis/follow.h"
#include "store/benchmark/async/retwis/get_timeline.h"
#include "store/benchmark/async/retwis/post_tweet.h"

#include <iostream>

namespace retwis {

RetwisClient::RetwisClient(KeySelector *keySelector, AsyncClient &client,
    Transport &transport, int numRequests, int expDuration, uint64_t delay,
    int warmupSec, int cooldownSec, int tputInterval,
    const std::string &latencyFilename)
    : AsyncTransactionBenchClient(client, transport, numRequests, expDuration,
        delay, warmupSec, cooldownSec, tputInterval, latencyFilename),
        keySelector(keySelector) {
}

RetwisClient::~RetwisClient() {
}

AsyncTransaction *RetwisClient::GetNextTransaction() {
  int ttype = std::rand() % 100;
  tid++;
  if (ttype < 5) {
    lastOp = "add_user";
    return new AddUser(tid, keySelector);
  } else if (ttype < 20) {
    lastOp = "follow";
    return new Follow(tid, keySelector);
  } else if (ttype < 50) {
    lastOp = "post_tweet";
    return new PostTweet(tid, keySelector);
  } else {
    lastOp = "get_timeline";
    return new GetTimeline(tid, keySelector);
  }
}

std::string RetwisClient::GetLastOp() const {
  return lastOp;
}

} //namespace retwis
