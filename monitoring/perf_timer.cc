#include "monitoring/perf_timer.h"
namespace rocksdb {

void PerfTimer::Stop(uint64_t start_time, uint32_t ticker_type) {
    if (start_time) {
      uint64_t duration = time_now() - start_time;
      if (statistics_ != nullptr) {
        if (duration > 1000000000ull) {
            printf("PerfTimer::Stop duration:%lld, ticker_type:%d \n", (long long)duration, ticker_type);
        } else {
            RecordTick(statistics_, ticker_type, duration);
        }
      }
    }
}

}