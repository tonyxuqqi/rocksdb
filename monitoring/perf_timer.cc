#include "monitoring/perf_timer.h"
namespace rocksdb {

void PerfTimer::Measure(uint64_t start_time, uint32_t ticker_type) {
  if (start_time) {
    uint64_t duration = time_now() - start_time;
    if (statistics_ != nullptr) {
      if (duration > 1000000000ull) {
          printf("PerfTimer::Measure duration:%lldms, ticker_type:%d \n", (long long)duration/1000000, ticker_type);
      }
      RecordTick(statistics_, ticker_type, duration);
    }
  }
}

}