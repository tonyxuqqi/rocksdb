#include "monitoring/perf_timer.h"
namespace rocksdb {

void PerfTimer::Stop() {
    if (start_record_) {
      uint64_t duration = time_now() - start_record_;
      if (statistics_ != nullptr) {
        if (duration > 1000000000ull) {
            printf("PerfTimer::Stop duration:%lld, ticker_type:%d \n", (long long)duration, ticker_type_);
        } else {
            RecordTick(statistics_, ticker_type_, duration);
        }
      }
      start_record_ = 0;
    }
}

}