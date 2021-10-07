#include "monitoring/perf_timer.h"
namespace rocksdb {

void PerfTimer::Stop(uint64_t start_record, uint32_t ticker_type) {
    if (start_record) {
      uint64_t duration = time_now() - start_record;
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