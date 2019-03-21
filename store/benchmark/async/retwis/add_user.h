#ifndef RETWIS_ADD_USER_H
#define RETWIS_ADD_USER_H

#include <functional>

#include "store/benchmark/async/retwis/retwis_transaction.h"

namespace retwis {

class AddUser : public RetwisTransaction {
 public:
  AddUser(uint64_t tid, Client *client, KeySelector *keySelector);
  virtual ~AddUser();
 
 protected:
  void ExecuteNextOperation();

};

} // namespace retwis

#endif /* RETWIS_ADD_USER_H */
