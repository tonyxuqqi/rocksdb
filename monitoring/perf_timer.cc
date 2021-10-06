#include "monitoring/perf_timer.h"
namespace rocksdb {

void PerfTimer::Stop() {
    if (start_) {
      uint64_t duration = time_now() - start_;
      if (statistics_ != nullptr) {
        if (duration > ULL(1000000000)) {
            printf("PerfTimer::Stop duration:%d, ticker_type:%d \n", duration, ticker_type_)
        } else {
            RecordTick(statistics_, ticker_type_, duration);
        }
      }
      start_ = 0;
    }
}

}