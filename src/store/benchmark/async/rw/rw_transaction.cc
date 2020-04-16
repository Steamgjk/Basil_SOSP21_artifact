#include "store/benchmark/async/rw/rw_transaction.h"

namespace rw {

RWTransaction::RWTransaction(KeySelector *keySelector, int numOps,
    std::mt19937 &rand) : keySelector(keySelector), numOps(numOps) {
  for (int i = 0; i < numOps; ++i) {
    uint64_t key;
    if (i % 2 == 0) {
      key = keySelector->GetKey(rand);
    } else {
      key = keyIdxs[i - 1];
    }
    keyIdxs.push_back(key);
  }
}

RWTransaction::~RWTransaction() {
}

Operation RWTransaction::GetNextOperation(size_t opCount,
    std::map<std::string, std::string> readValues) {
  if (opCount < GetNumOps()) {
    if (opCount % 2 == 0) {
      return Get(GetKey(opCount));
    } else {
      auto strValueItr = readValues.find(GetKey(opCount));
      UW_ASSERT(strValueItr != readValues.end());
      std::string strValue = strValueItr->second;
      std::string writeValue;
      if (strValue.length() == 0) {
        writeValue = std::string(4, '\0');
      } else {
        uint64_t intValue = 0;
        for (int i = 0; i < 4; ++i) {
          intValue = intValue | (static_cast<uint64_t>(strValue[i]) << ((3 - i) * 8));
        }
        intValue++;
        for (int i = 0; i < 4; ++i) {
          writeValue += static_cast<char>((intValue >> (3 - i) * 8) & 0xFF);
        }
      }
      return Put(GetKey(opCount), writeValue);
    }
  } else {
    return Commit();
  }
}

} // namespace rw
