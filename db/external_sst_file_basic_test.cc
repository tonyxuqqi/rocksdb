//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <functional>

#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/slice.h"
#include "test_util/fault_injection_test_env.h"
#include "test_util/testutil.h"
#include <iostream>
#include "rocksdb/options.h"
#include <algorithm>
#include <random>
#include <chrono>
namespace rocksdb {

#ifndef ROCKSDB_LITE
class ExternalSSTFileBasicTest
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  ExternalSSTFileBasicTest() : DBTestBase("/external_sst_file_basic_test") {
    sst_files_dir_ = dbname_ + "/sst_files/";
    fault_injection_test_env_.reset(new FaultInjectionTestEnv(Env::Default()));
    DestroyAndRecreateExternalSSTFilesDir();
  }

  void DestroyAndRecreateExternalSSTFilesDir() {
    test::DestroyDir(env_, sst_files_dir_);
    env_->CreateDir(sst_files_dir_);
  }

  Status DeprecatedAddFile(const std::vector<std::string>& files,
                           bool move_files = false,
                           bool skip_snapshot_check = false) {
    (void)skip_snapshot_check;
    IngestExternalFileOptions opts;
    opts.move_files = move_files;
    opts.snapshot_consistency = !skip_snapshot_check;
    opts.allow_global_seqno = true;
    opts.allow_blocking_flush = false;
    opts.write_global_seqno = false;
    return db_->IngestExternalFile(files, opts);
  }

  Status DeprecatedAddFile(DB* db, ColumnFamilyHandle* handle, const std::vector<std::string>& files,
                           bool move_files = false,
                           bool skip_snapshot_check = false) {
    (void)skip_snapshot_check;
    IngestExternalFileOptions opts;
    opts.move_files = move_files;
    opts.snapshot_consistency = !skip_snapshot_check;
    opts.allow_global_seqno = true;
    opts.allow_blocking_flush = false;
    opts.write_global_seqno = false;
    return db->IngestExternalFile(handle, files, opts);
  }

  Status GenerateAndAddExternalFile(
      const Options options, std::vector<int> keys,
      const std::vector<ValueType>& value_types,
      std::vector<std::pair<int, int>> range_deletions, int file_id,
      bool write_global_seqno, bool verify_checksums_before_ingest,
      std::map<std::string, std::string>* true_data) {
    assert(value_types.size() == 1 || keys.size() == value_types.size());
    std::string file_path = sst_files_dir_ + ToString(file_id);
    SstFileWriter sst_file_writer(EnvOptions(), options);

    Status s = sst_file_writer.Open(file_path);
    if (!s.ok()) {
      return s;
    }
    for (size_t i = 0; i < range_deletions.size(); i++) {
      // Account for the effect of range deletions on true_data before
      // all point operators, even though sst_file_writer.DeleteRange
      // must be called before other sst_file_writer methods. This is
      // because point writes take precedence over range deletions
      // in the same ingested sst.
      std::string start_key = Key(range_deletions[i].first);
      std::string end_key = Key(range_deletions[i].second);
      s = sst_file_writer.DeleteRange(start_key, end_key);
      if (!s.ok()) {
        sst_file_writer.Finish();
        return s;
      }
      auto start_key_it = true_data->find(start_key);
      if (start_key_it == true_data->end()) {
        start_key_it = true_data->upper_bound(start_key);
      }
      auto end_key_it = true_data->find(end_key);
      if (end_key_it == true_data->end()) {
        end_key_it = true_data->upper_bound(end_key);
      }
      true_data->erase(start_key_it, end_key_it);
    }
    for (size_t i = 0; i < keys.size(); i++) {
      std::string key = Key(keys[i]);
      std::string value = Key(keys[i]) + ToString(file_id);
      ValueType value_type =
          (value_types.size() == 1 ? value_types[0] : value_types[i]);
      switch (value_type) {
        case ValueType::kTypeValue:
          s = sst_file_writer.Put(key, value);
          (*true_data)[key] = value;
          break;
        case ValueType::kTypeMerge:
          s = sst_file_writer.Merge(key, value);
          // we only use TestPutOperator in this test
          (*true_data)[key] = value;
          break;
        case ValueType::kTypeDeletion:
          s = sst_file_writer.Delete(key);
          true_data->erase(key);
          break;
        default:
          return Status::InvalidArgument("Value type is not supported");
      }
      if (!s.ok()) {
        sst_file_writer.Finish();
        return s;
      }
    }
    s = sst_file_writer.Finish();

    if (s.ok()) {
      IngestExternalFileOptions ifo;
      ifo.allow_global_seqno = true;
      ifo.write_global_seqno = write_global_seqno;
      ifo.verify_checksums_before_ingest = verify_checksums_before_ingest;
      s = db_->IngestExternalFile({file_path}, ifo);
    }
    return s;
  }

  Status GenerateAndAddExternalFile(
      const Options options, std::vector<int> keys,
      const std::vector<ValueType>& value_types, int file_id,
      bool write_global_seqno, bool verify_checksums_before_ingest,
      std::map<std::string, std::string>* true_data) {
    return GenerateAndAddExternalFile(
        options, keys, value_types, {}, file_id, write_global_seqno,
        verify_checksums_before_ingest, true_data);
  }

  Status GenerateAndAddExternalFile(
      const Options options, std::vector<int> keys, const ValueType value_type,
      int file_id, bool write_global_seqno, bool verify_checksums_before_ingest,
      std::map<std::string, std::string>* true_data) {
    return GenerateAndAddExternalFile(
        options, keys, std::vector<ValueType>(1, value_type), file_id,
        write_global_seqno, verify_checksums_before_ingest, true_data);
  }

  void IngestByLevels(DB* src_db, DB* target_db, const std::string& src_db_path,
                      const std::vector<std::string>& cfs,
                      std::vector<ColumnFamilyHandle*>* src_handles,
                      std::vector<ColumnFamilyHandle*>* target_handles,
                      bool print) {
    for (size_t i = 0; i < src_handles->size(); i++) { 
      ColumnFamilyMetaData cf_meta;
      src_db->GetColumnFamilyMetaData((*src_handles)[i], &cf_meta);
      for (int idx = (int)cf_meta.levels.size() -1; idx >= 0; idx--) {
        LevelMetaData& level = cf_meta.levels[idx];
        std::vector<std::string> input_file_names;
        for (auto file : level.files) {
          if (print) {
            std::cout << cfs[i] << " level " << level.level << " " << file.name << std::endl;
          }
          if (cfs[i] == "raft") {
            continue;
          }
          if (level.level != 0) {
            input_file_names.push_back(src_db_path + file.name);
          } else {
            (void)target_db;
            Status s = DeprecatedAddFile(target_db, (*target_handles)[i], {src_db_path + file.name});
            if (!s.ok()) {
               target_db->Flush(FlushOptions(), (*target_handles)[i]);
               s = DeprecatedAddFile(target_db, (*target_handles)[i], {src_db_path + file.name});
            }
            ASSERT_TRUE(s.ok());
            if (!s.ok()) {
              std::cout << s.ToString();
            }
          }
        }
        if (input_file_names.size() != 0) {
          Status s = DeprecatedAddFile(target_db, (*target_handles)[i], input_file_names);
          if (!s.ok()) {
              target_db->Flush(FlushOptions(), (*target_handles)[i]);
              s = DeprecatedAddFile(target_db, (*target_handles)[i], input_file_names);
          }
          ASSERT_TRUE(s.ok());
          if (!s.ok()) {
            std::cout << s.ToString();
          }
        }
      }
    }
  }
  
  int GetKeyCount(DB* db,  ColumnFamilyHandle* handle, const std::string& start, const std::string& end, bool print = false) {
      std::string start_key = std::string(1, 'z') + start;
      std::string end_key = end.empty() ? std::string(1, 'z' + 1) :  std::string(1, 'z') + end; 
      int key_count = 0;
      int outofrange_key_count = 0;
      ReadOptions opt;
      const std::unique_ptr<rocksdb::Iterator> it(
          db->NewIterator(opt, handle));
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        if (key < end_key && key >= start_key) {
          key_count++;
          if (print) {
            std::cout << "key " << key << "  " << it->value().ToString() << std::endl;
          }
        } else {
          outofrange_key_count++;
          //std::cout << "out of range key:" << key << std::endl;
        }
      }
      std:: cout << "out of range key " << outofrange_key_count << std::endl; 
      return key_count; 
  }

  void VerifyKeysCount(DB* target_db, const std::vector<std::string>& cfs,
                       std::vector<ColumnFamilyHandle*>* target_handles,
                       int expectedKeys[4],
                       const std::string& start,
                       const std::string& end, 
                       bool print = false) {
    for (size_t j = 0; j < target_handles->size(); j++) {
      if (cfs[j] == "raft") {
        continue;
      }
      uint64_t key_count = GetKeyCount(target_db, (*target_handles)[j], start, end, print && j == 1);
      ASSERT_EQ(key_count, expectedKeys[j]);
    }
  }

  void DeleteRange(DB* db, ColumnFamilyHandle* handle, Slice& start, Slice& end) {
    const std::unique_ptr<rocksdb::Iterator> it(
      db->NewIterator(rocksdb::ReadOptions(), handle));
    it->Seek(start);
    for (; it->Valid() && it->key().ToString() < end.ToString() ; it->Next()) {
       db->Delete(WriteOptions(), handle, it->key());
    }
    /*if (start.ToString() < end.ToString()) {
      Status s = db->DeleteRange(WriteOptions(), handle, start, end);
      if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
      }
      ASSERT_TRUE(s.ok());
    }*/
    Status s = db->CompactRange(CompactRangeOptions(), handle, &start, &end);
    ASSERT_TRUE(s.ok());
  }
  void CleanData(DB* db, std::vector<ColumnFamilyHandle*> handles, const std::string& start, const std::string& end) {
    (void)db;
    std::string start_key = "z" + start;
    std::string end_key = "z" + end;
    if (end.empty()) {
      end_key = std::string(1, 'z'+1);
    }
    for(size_t i = 0; i < handles.size(); i++) {
        Slice startSlice("z");
        Slice s1(start_key);
        Slice s2(end_key);
        Slice endSlice(std::string(1, 'z' + 1));
        DeleteRange(db, handles[i], startSlice, s1);
        DeleteRange(db, handles[i], s2, endSlice);
    }
  }

 

  ~ExternalSSTFileBasicTest() override {
    std::cout << "sst_files_dir_ " << sst_files_dir_ << std::endl;
    test::DestroyDir(env_, sst_files_dir_);
  }

 protected:
  std::string sst_files_dir_;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_test_env_;
};

TEST_F(ExternalSSTFileBasicTest, Basic) {
  Options options = CurrentOptions();

  SstFileWriter sst_file_writer(EnvOptions(), options);

  // Current file size should be 0 after sst_file_writer init and before open a
  // file.
  ASSERT_EQ(sst_file_writer.FileSize(), 0);

  // file1.sst (0 => 99)
  std::string file1 = sst_files_dir_ + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 0; k < 100; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file1_info;
  Status s = sst_file_writer.Finish(&file1_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  std::cout << "sst_file " << file1 << std::endl;

  // Current file size should be non-zero after success write.
  ASSERT_GT(sst_file_writer.FileSize(), 0);

  ASSERT_EQ(file1_info.file_path, file1);
  ASSERT_EQ(file1_info.num_entries, 100);
  ASSERT_EQ(file1_info.smallest_key, Key(0));
  ASSERT_EQ(file1_info.largest_key, Key(99));
  ASSERT_EQ(file1_info.num_range_del_entries, 0);
  ASSERT_EQ(file1_info.smallest_range_del_key, "");
  ASSERT_EQ(file1_info.largest_range_del_key, "");
  // sst_file_writer already finished, cannot add this value
  s = sst_file_writer.Put(Key(100), "bad_val");
  ASSERT_FALSE(s.ok()) << s.ToString();
  s = sst_file_writer.DeleteRange(Key(100), Key(200));
  ASSERT_FALSE(s.ok()) << s.ToString();

  DestroyAndReopen(options);
  db_->Put(rocksdb::WriteOptions(), "zk4", "v4");
  db_->Flush(FlushOptions());
  // Add file using file path
  s = DeprecatedAddFile({file1});
  std::vector<std::string> livefiles;
  uint64_t manifest_file_size;
  db_->GetLiveFiles(livefiles, &manifest_file_size, false);
  for (size_t i = 0; i < livefiles.size(); i++) {
    std::cout << "file " << livefiles[i] << std::endl;
  }
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = DeprecatedAddFile({"/Users/qixu/l/rocksdb/000016.sst"});
  ASSERT_TRUE(s.ok()) << s.ToString();
  // ASSERT_EQ(db_->GetLatestSequenceNumber(), 2U);
  livefiles.clear();
  db_->GetLiveFiles(livefiles, &manifest_file_size, false);
  for (size_t i = 0; i < livefiles.size(); i++) {
    std::cout << "file " << livefiles[i] << std::endl;
  }
  TryReopen(options);
  livefiles.clear();
  db_->GetLiveFiles(livefiles, &manifest_file_size, false);
  for (size_t i = 0; i < livefiles.size(); i++) {
    std::cout << "file " << livefiles[i] << std::endl;
  }
  // ASSERT_EQ(Get("zk4"), "v4");
  // ASSERT_EQ(Get(Key(0)), Key(0) + "_val");
  ASSERT_EQ(Get("zk3"), "v3");
  ASSERT_EQ(Get("zk1"), "v1");
  db_->Put(rocksdb::WriteOptions(), "zk3", "vv3");
  db_->Put(rocksdb::WriteOptions(), "zk1", "vv1");
  db_->Put(rocksdb::WriteOptions(), "zk3", "vvv3");
  db_->Put(rocksdb::WriteOptions(), "zk1", "vvv1");
  ASSERT_EQ(Get("zk3"), "vvv3");
  ASSERT_EQ(Get("zk1"), "vvv1");
  db_->Flush(FlushOptions());

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);

  std::vector<std::string> input_file_names;
  for (auto level : cf_meta.levels) {
    for (auto file : level.files) {
      input_file_names.push_back(file.name);
    }
  }
  CompactionOptions compact_options;
  s = db_->CompactFiles(compact_options, input_file_names, 3);
  ASSERT_TRUE(s.ok());
  for (int k = 0; k < 100; k++) {
    ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
  }
  ASSERT_EQ(Get("zk3"), "vvv3");
  ASSERT_EQ(Get("zk1"), "vvv1");
}

TEST_F(ExternalSSTFileBasicTest, DirectIngest) {
  Options options = CurrentOptions();
  options.force_consistency_checks = true;

  DestroyAndReopen(options);
  std::string ssts[] = {"/Users/qixu/l/rocksdb/sst/000014.sst",
                        "/Users/qixu/l/rocksdb/sst/000007.sst",
                        "/Users/qixu/l/rocksdb/sst/000018.sst",
                        "/Users/qixu/l/rocksdb/sst/000010.sst",
                        "/Users/qixu/l/rocksdb/sst/000012.sst",
                        "/Users/qixu/l/rocksdb/sst/000016.sst",
                        "/Users/qixu/l/rocksdb/sst/000020.sst",
                        "/Users/qixu/l/rocksdb/sst/000022.sst",
                        "/Users/qixu/l/rocksdb/sst/000024.sst",
                        "/Users/qixu/l/rocksdb/sst/000026.sst"};
  std::vector<int> indx;
  for (int i = 0; i < 10; i++) {
    indx.push_back(i);
  }
  unsigned seed =
      (unsigned)std::chrono::system_clock::now().time_since_epoch().count();
  shuffle(indx.begin(), indx.end(), std::default_random_engine(seed));
  for (int t = 0; t < 20; t++) {
    for (int i = 0; i < 100; i++) {
      db_->Put(rocksdb::WriteOptions(), Key(i + t * 100), Key(i + t * 100));
    }
    db_->Flush(FlushOptions());
  }

  for (int i = 0; i < 10; i++) {
    std::cout << "ingesting " << ssts[indx[i]] << std::endl;
    Status s = DeprecatedAddFile({ssts[indx[i]]});
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);

  std::vector<std::string> input_file_names;
  for (auto level : cf_meta.levels) {
    std::cout << "level " << level.level << std::endl;
    for (auto file : level.files) {
      input_file_names.push_back(file.name);
      std::cout << file.name << "  smallest seqno:" << file.smallest_seqno
                << "   largest seqno: " << file.largest_seqno << std::endl;
    }
  }
  CompactionOptions compact_options;
  Status s = db_->CompactFiles(compact_options, input_file_names, 6);
  ASSERT_TRUE(s.ok());

  for (int t = 0; t < 20; t++) {
    for (int i = 0; i < 100; i++) {
      ASSERT_EQ(Get(Key(i + t * 100)), Key(i + t * 100));
    }
  }
  /*std::vector<std::string> livefiles;
  uint64_t manifest_file_size;
  db_->GetLiveFiles(livefiles, &manifest_file_size, false);
  for (size_t i = 0; i < livefiles.size(); i++) {
      std::cout << "file " << livefiles[i] << std::endl;
  }*/
}

TEST_F(ExternalSSTFileBasicTest, DirectIngest2) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  // options.force_consistency_checks = true;
  std::string root =
      "/data4/qixu/l/rust-rocksdb/librocksdb_sys/rocksdb/sst/tablets/";
  std::string db_folders[] = {
      "10_9", "12_9", "16_10", "18_17", "20_9",  "22_27", "24_9", "2_60",
      "26_9", "28_9", "32_10", "36_9",  "38_9",  "42_9",  "44_9", "46_9",
      "48_9", "50_9", "56_9",  "60_10", "6_210", "64_9",  "66_9", "8_11",
  };
  const int merge_count = 24;
  std::string db_merges[] = {
      "26 24", "44 46", "32 36", "60 66", "12 10", "18 16", "38 28", "2 64",
      "66 56", "6 8",   "16 20", "24 28", "46 42", "50 48", "56 64", "10 20",
      "28 36", "42 48", "64 52", "22 20", "48 36", "8 20",  "36 52", "20 52",
  };

  std::map<int, std::vector<std::string>> ranges_map = {
    {8,
     {"7480000000000000FF0500000000000000F8",
      "7480000000000000FF0700000000000000F8"}},
     {28,
      {"7480000000000000FF1700000000000000F8",
       "7480000000000000FF1900000000000000F8"}},
      {38,
       {"7480000000000000FF1900000000000000F8",
        "7480000000000000FF1B00000000000000F8"}},
       {48,
        {"7480000000000000FF2500000000000000F8",
         "7480000000000000FF2700000000000000F8"}},
        {52,
         {"7480000000000000FF2900000000000000F8",
          "7480000000000000FF2B00000000000000F8"}},
         {60,
          {"7480000000000000FF2F00000000000000F8",
           "7480000000000000FF3100000000000000F8"}},
          {66,
           {"7480000000000000FF2D00000000000000F8",
            "7480000000000000FF2F00000000000000F8"}},
           {2, {"7480000000000000FF3300000000000000F8", ""}},
            {16,
             {"7480000000000000FF0D00000000000000F8",
              "7480000000000000FF0F00000000000000F8"}},
             {26,
              {"7480000000000000FF1500000000000000F8",
               "7480000000000000FF1700000000000000F8"}},
              {32,
               {"7480000000000000FF1B00000000000000F8",
                "7480000000000000FF1D00000000000000F8"}},
               {42,
                {"7480000000000000FF1F00000000000000F8",
                 "7480000000000000FF2100000000000000F8"}},
                {50,
                 {"7480000000000000FF2700000000000000F8",
                  "7480000000000000FF2900000000000000F8"}},
                 {10,
                  {"7480000000000000FF0700000000000000F8",
                   "7480000000000000FF0900000000000000F8"}},
                  {12,
                   {"7480000000000000FF0900000000000000F8",
                    "7480000000000000FF0B00000000000000F8"}},
                   {20,
                    {"7480000000000000FF0B00000000000000F8",
                     "7480000000000000FF0D00000000000000F8"}},
                    {22,
                     {"7480000000000000FF1100000000000000F8",
                      "7480000000000000FF1300000000000000F8"}},
                     {44,
                      {"7480000000000000FF2100000000000000F8",
                       "7480000000000000FF2300000000000000F8"}},
                      {56,
                       {"7480000000000000FF2B00000000000000F8",
                        "7480000000000000FF2D00000000000000F8"}},
                       {64,
                        {"7480000000000000FF3100000000000000F8",
                         "7480000000000000FF3300000000000000F8"}},
                        {6, {"", "7480000000000000FF0500000000000000F8"}},
                         {18,
                          {"7480000000000000FF0F00000000000000F8",
                           "7480000000000000FF1100000000000000F8"}},
                          {24,
                           {"7480000000000000FF1300000000000000F8",
                            "7480000000000000FF1500000000000000F8"}},
                           {36,
                            {"7480000000000000FF1D00000000000000F8",
                             "7480000000000000FF1F00000000000000F8"}},
                            {46,
                             {"7480000000000000FF2300000000000000F8",
                              "7480000000000000FF2500000000000000F8"}}};
  std::map<int, int[4]> keyCountMap;
  std::map<int, int> regionMap;
  std::vector<DB*> dbs;
  std::vector<std::vector<ColumnFamilyHandle*>> cf_handles_list;
  std::vector<std::string> cfs;
  DB::ListColumnFamilies(
      DBOptions(),
      std::string(
          root) +
          db_folders[0],
      &cfs);
  std::vector<ColumnFamilyDescriptor> cfds;
  for (size_t i = 0; i < cfs.size(); i++) {
    cfds.push_back(ColumnFamilyDescriptor(cfs[i], ColumnFamilyOptions()));
  }
  for (int i = 0; i < 24; i++) {
    DB* src_db;
    std::vector<ColumnFamilyHandle*> handles;
    Status s = TryReopen(
        options,
        std::string(
            "/data4/qixu/l/rust-rocksdb/librocksdb_sys/rocksdb/sst/tablets/") +
            db_folders[i],
        cfds, &handles, &src_db);
    dbs.push_back(src_db);
    cf_handles_list.push_back(handles);
    ASSERT_TRUE(s.ok()) << s.ToString();
    int region_id =
        atoi(db_folders[i].substr(0, db_folders[i].find("_")).c_str());
    regionMap[region_id] = i;
    // keyCountMap[region_id];
    for (size_t j = 0; j < cfs.size(); j++) {
      if (cfs[j] == "raft") {
        keyCountMap[region_id][j] = 0;
        continue;
      }
      std::string start = ranges_map[region_id][0];
      std::string end = ranges_map[region_id][1]; 
      Slice ss(start);
      std::string src_start_key;
      std::string src_end_key;
      ss.DecodeHex(&src_start_key);
      Slice tt(end);
      tt.DecodeHex(&src_end_key);
      int key_count = GetKeyCount(src_db, handles[j], src_start_key, src_end_key, region_id == 50 && j == 1); 
      std::cout << "region_id " << region_id << "  cf " << cfs[j]
                << " key count " << key_count << std::endl;
      keyCountMap[region_id][j] = key_count;
    }
  }

  for (int i = 0; i < merge_count; i++) {
    int source_region =
        atoi(db_merges[i].substr(0, db_merges[i].find(' ')).c_str());
    int target_region =
        atoi(db_merges[i].substr(db_merges[i].find(' ') + 1).c_str());
    std::string start = ranges_map[source_region][0];
    std::string end = ranges_map[source_region][1]; 
    std::string src_start_key;
    std::string src_end_key;
    std::string target_end_key;
    std::string target_start_key;
    Slice ss(start);
    ss.DecodeHex(&src_start_key);
    Slice tt(end);
    tt.DecodeHex(&src_end_key);
    CleanData(dbs[regionMap[source_region]], cf_handles_list[regionMap[source_region]], src_start_key, src_end_key);
    for (size_t j = 0; j < cfs.size(); j++) {
      if (cfs[j] == "raft") {
        keyCountMap[source_region][j] = 0;
        continue;
      }
      int key_count = GetKeyCount(dbs[regionMap[source_region]], cf_handles_list[regionMap[source_region]][j], src_start_key, src_end_key); 
      std::cout << "after compaction region_id " << source_region << "  cf " << cfs[j]
                << " key count " << key_count << std::endl;
      keyCountMap[source_region][j] = key_count;
    }

    start = ranges_map[target_region][0];
    end = ranges_map[target_region][1];
    Slice ss1(start);
    ss1.DecodeHex(&target_start_key);
    Slice tt1(end);
    tt1.DecodeHex(&target_end_key);
    CleanData(dbs[regionMap[target_region]], cf_handles_list[regionMap[target_region]], target_start_key, target_end_key); 
    for (size_t j = 0; j < cfs.size(); j++) {
      if (cfs[j] == "raft") {
        keyCountMap[target_region][j] = 0;
        continue;
      }
      int key_count = GetKeyCount(dbs[regionMap[target_region]], cf_handles_list[regionMap[target_region]][j], target_start_key, target_end_key); 
      std::cout << "after compaction region_id " << target_region << "  cf " << cfs[j]
                << " key count " << key_count << std::endl;
      keyCountMap[target_region][j] = key_count;
    }

    IngestByLevels(dbs[regionMap[source_region]], dbs[regionMap[target_region]], root + db_folders[regionMap[source_region]],
                   cfs, &cf_handles_list[regionMap[source_region]], &cf_handles_list[regionMap[target_region]], source_region == 50 && target_region == 48);
    int expectedKeys[4];
    for (int j = 0; j < 4; j++) {
      expectedKeys[j] =
          keyCountMap[source_region][j] + keyCountMap[target_region][j];
    }
    
    // update range map
    if (src_start_key > target_start_key) {
      ranges_map[target_region][1] = ranges_map[source_region][1];
      target_end_key = src_end_key;
    } else {
       ranges_map[target_region][0] = ranges_map[source_region][0];
       target_start_key = src_start_key;
    }

    VerifyKeysCount(dbs[regionMap[target_region]], cfs,
                    &cf_handles_list[regionMap[target_region]], expectedKeys, target_start_key, target_end_key, target_region == 48);
    
   
  }

  for (int i = 0; i < 24; i++) {
    for (int j = 0; j < 4; j++) {
      delete cf_handles_list[i][j];
      cf_handles_list[i].clear();
    }
  }
}

TEST_F(ExternalSSTFileBasicTest, NoCopy) {
  Options options = CurrentOptions();
  const ImmutableCFOptions ioptions(options);

  SstFileWriter sst_file_writer(EnvOptions(), options);

  // file1.sst (0 => 99)
  std::string file1 = sst_files_dir_ + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 0; k < 100; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file1_info;
  Status s = sst_file_writer.Finish(&file1_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(file1_info.file_path, file1);
  ASSERT_EQ(file1_info.num_entries, 100);
  ASSERT_EQ(file1_info.smallest_key, Key(0));
  ASSERT_EQ(file1_info.largest_key, Key(99));

  // file2.sst (100 => 299)
  std::string file2 = sst_files_dir_ + "file2.sst";
  ASSERT_OK(sst_file_writer.Open(file2));
  for (int k = 100; k < 300; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file2_info;
  s = sst_file_writer.Finish(&file2_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(file2_info.file_path, file2);
  ASSERT_EQ(file2_info.num_entries, 200);
  ASSERT_EQ(file2_info.smallest_key, Key(100));
  ASSERT_EQ(file2_info.largest_key, Key(299));

  // file3.sst (110 => 124) .. overlap with file2.sst
  std::string file3 = sst_files_dir_ + "file3.sst";
  ASSERT_OK(sst_file_writer.Open(file3));
  for (int k = 110; k < 125; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val_overlap"));
  }
  ExternalSstFileInfo file3_info;
  s = sst_file_writer.Finish(&file3_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(file3_info.file_path, file3);
  ASSERT_EQ(file3_info.num_entries, 15);
  ASSERT_EQ(file3_info.smallest_key, Key(110));
  ASSERT_EQ(file3_info.largest_key, Key(124));

  s = DeprecatedAddFile({file1}, true /* move file */);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(Status::NotFound(), env_->FileExists(file1));

  s = DeprecatedAddFile({file2}, false /* copy file */);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_OK(env_->FileExists(file2));

  // This file has overlapping values with the existing data
  s = DeprecatedAddFile({file3}, true /* move file */);
  ASSERT_FALSE(s.ok()) << s.ToString();
  ASSERT_OK(env_->FileExists(file3));

  for (int k = 0; k < 300; k++) {
    ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
  }
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithGlobalSeqnoPickedSeqno) {
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  do {
    Options options = CurrentOptions();
    DestroyAndReopen(options);
    std::map<std::string, std::string> true_data;

    int file_id = 1;

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 2, 3, 4, 5, 6}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {10, 11, 12, 13}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 4, 6}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {11, 15, 19}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {120, 130}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 130}, ValueType::kTypeValue, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3);

    // Write some keys through normal write path
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(i), "memtable"));
      true_data[Key(i)] = "memtable";
    }
    SequenceNumber last_seqno = dbfull()->GetLatestSequenceNumber();

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {60, 61, 62}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {40, 41, 42}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {20, 30, 40}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 2);

    const Snapshot* snapshot = db_->GetSnapshot();

    // We will need a seqno for the file regardless if the file overwrite
    // keys in the DB or not because we have a snapshot
    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1000, 1002}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {2000, 3002}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 20, 40, 100, 150}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    db_->ReleaseSnapshot(snapshot);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {5000, 5001}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // No snapshot anymore, no need to assign a seqno
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    size_t kcnt = 0;
    VerifyDBFromMap(true_data, &kcnt, false);
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithMultipleValueType) {
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  do {
    Options options = CurrentOptions();
    options.merge_operator.reset(new TestPutOperator());
    DestroyAndReopen(options);
    std::map<std::string, std::string> true_data;

    int file_id = 1;

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 2, 3, 4, 5, 6}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {10, 11, 12, 13}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 4, 6}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {11, 15, 19}, ValueType::kTypeDeletion, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {120, 130}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 130}, ValueType::kTypeDeletion, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {120}, {ValueType::kTypeValue}, {{120, 135}}, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {}, {}, {{110, 120}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // The range deletion ends on a key, but it doesn't actually delete
    // this key because the largest key in the range is exclusive. Still,
    // it counts as an overlap so a new seqno will be assigned.
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 5);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {}, {}, {{100, 109}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 5);

    // Write some keys through normal write path
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(i), "memtable"));
      true_data[Key(i)] = "memtable";
    }
    SequenceNumber last_seqno = dbfull()->GetLatestSequenceNumber();

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {60, 61, 62}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {40, 41, 42}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {20, 30, 40}, ValueType::kTypeDeletion, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 2);

    const Snapshot* snapshot = db_->GetSnapshot();

    // We will need a seqno for the file regardless if the file overwrite
    // keys in the DB or not because we have a snapshot
    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1000, 1002}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {2000, 3002}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 20, 40, 100, 150}, ValueType::kTypeMerge, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    db_->ReleaseSnapshot(snapshot);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {5000, 5001}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data));
    // No snapshot anymore, no need to assign a seqno
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    size_t kcnt = 0;
    VerifyDBFromMap(true_data, &kcnt, false);
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithMixedValueType) {
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  do {
    Options options = CurrentOptions();
    options.merge_operator.reset(new TestPutOperator());
    DestroyAndReopen(options);
    std::map<std::string, std::string> true_data;

    int file_id = 1;

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 2, 3, 4, 5, 6},
        {ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeValue,
         ValueType::kTypeMerge, ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {10, 11, 12, 13},
        {ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeValue,
         ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 0);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 4, 6},
        {ValueType::kTypeDeletion, ValueType::kTypeValue,
         ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {11, 15, 19},
        {ValueType::kTypeDeletion, ValueType::kTypeMerge,
         ValueType::kTypeValue},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {120, 130}, {ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 2);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 130}, {ValueType::kTypeMerge, ValueType::kTypeDeletion},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {150, 151, 152},
        {ValueType::kTypeValue, ValueType::kTypeMerge,
         ValueType::kTypeDeletion},
        {{150, 160}, {180, 190}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {150, 151, 152},
        {ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeValue},
        {{200, 250}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {300, 301, 302},
        {ValueType::kTypeValue, ValueType::kTypeMerge,
         ValueType::kTypeDeletion},
        {{1, 2}, {152, 154}}, file_id++, write_global_seqno,
        verify_checksums_before_ingest, &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 5);

    // Write some keys through normal write path
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(i), "memtable"));
      true_data[Key(i)] = "memtable";
    }
    SequenceNumber last_seqno = dbfull()->GetLatestSequenceNumber();

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {60, 61, 62},
        {ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeValue},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File doesn't overwrite any keys, no seqno needed
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {40, 41, 42},
        {ValueType::kTypeValue, ValueType::kTypeDeletion,
         ValueType::kTypeDeletion},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 1);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {20, 30, 40},
        {ValueType::kTypeDeletion, ValueType::kTypeDeletion,
         ValueType::kTypeDeletion},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // File overwrites some keys, a seqno will be assigned
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 2);

    const Snapshot* snapshot = db_->GetSnapshot();

    // We will need a seqno for the file regardless if the file overwrite
    // keys in the DB or not because we have a snapshot
    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1000, 1002}, {ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 3);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {2000, 3002}, {ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 4);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {1, 20, 40, 100, 150},
        {ValueType::kTypeDeletion, ValueType::kTypeDeletion,
         ValueType::kTypeValue, ValueType::kTypeMerge, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // A global seqno will be assigned anyway because of the snapshot
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    db_->ReleaseSnapshot(snapshot);

    ASSERT_OK(GenerateAndAddExternalFile(
        options, {5000, 5001}, {ValueType::kTypeValue, ValueType::kTypeMerge},
        file_id++, write_global_seqno, verify_checksums_before_ingest,
        &true_data));
    // No snapshot anymore, no need to assign a seqno
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno + 5);

    size_t kcnt = 0;
    VerifyDBFromMap(true_data, &kcnt, false);
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_F(ExternalSSTFileBasicTest, FadviseTrigger) {
  Options options = CurrentOptions();
  const int kNumKeys = 10000;

  size_t total_fadvised_bytes = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SstFileWriter::Rep::InvalidatePageCache", [&](void* arg) {
        size_t fadvise_size = *(reinterpret_cast<size_t*>(arg));
        total_fadvised_bytes += fadvise_size;
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  std::unique_ptr<SstFileWriter> sst_file_writer;

  std::string sst_file_path = sst_files_dir_ + "file_fadvise_disable.sst";
  sst_file_writer.reset(
      new SstFileWriter(EnvOptions(), options, nullptr, false));
  ASSERT_OK(sst_file_writer->Open(sst_file_path));
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(sst_file_writer->Put(Key(i), Key(i)));
  }
  ASSERT_OK(sst_file_writer->Finish());
  // fadvise disabled
  ASSERT_EQ(total_fadvised_bytes, 0);

  sst_file_path = sst_files_dir_ + "file_fadvise_enable.sst";
  sst_file_writer.reset(
      new SstFileWriter(EnvOptions(), options, nullptr, true));
  ASSERT_OK(sst_file_writer->Open(sst_file_path));
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(sst_file_writer->Put(Key(i), Key(i)));
  }
  ASSERT_OK(sst_file_writer->Finish());
  // fadvise enabled
  ASSERT_EQ(total_fadvised_bytes, sst_file_writer->FileSize());
  ASSERT_GT(total_fadvised_bytes, 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(ExternalSSTFileBasicTest, SyncFailure) {
  Options options;
  options.create_if_missing = true;
  options.env = fault_injection_test_env_.get();

  std::vector<std::pair<std::string, std::string>> test_cases = {
      {"ExternalSstFileIngestionJob::BeforeSyncIngestedFile",
       "ExternalSstFileIngestionJob::AfterSyncIngestedFile"},
      {"ExternalSstFileIngestionJob::BeforeSyncDir",
       "ExternalSstFileIngestionJob::AfterSyncDir"},
      {"ExternalSstFileIngestionJob::BeforeSyncGlobalSeqno",
       "ExternalSstFileIngestionJob::AfterSyncGlobalSeqno"}};

  for (size_t i = 0; i < test_cases.size(); i++) {
    SyncPoint::GetInstance()->SetCallBack(test_cases[i].first, [&](void*) {
      fault_injection_test_env_->SetFilesystemActive(false);
    });
    SyncPoint::GetInstance()->SetCallBack(test_cases[i].second, [&](void*) {
      fault_injection_test_env_->SetFilesystemActive(true);
    });
    SyncPoint::GetInstance()->EnableProcessing();

    DestroyAndReopen(options);
    if (i == 2) {
      ASSERT_OK(Put("foo", "v1"));
    }

    Options sst_file_writer_options;
    std::unique_ptr<SstFileWriter> sst_file_writer(
        new SstFileWriter(EnvOptions(), sst_file_writer_options));
    std::string file_name =
        sst_files_dir_ + "sync_failure_test_" + ToString(i) + ".sst";
    ASSERT_OK(sst_file_writer->Open(file_name));
    ASSERT_OK(sst_file_writer->Put("bar", "v2"));
    ASSERT_OK(sst_file_writer->Finish());

    IngestExternalFileOptions ingest_opt;
    if (i == 0) {
      ingest_opt.move_files = true;
    }
    const Snapshot* snapshot = db_->GetSnapshot();
    if (i == 2) {
      ingest_opt.write_global_seqno = true;
    }
    ASSERT_FALSE(db_->IngestExternalFile({file_name}, ingest_opt).ok());
    db_->ReleaseSnapshot(snapshot);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    Destroy(options);
  }
}

TEST_P(ExternalSSTFileBasicTest, IngestionWithRangeDeletions) {
  int kNumLevels = 7;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = kNumLevels;
  Reopen(options);

  std::map<std::string, std::string> true_data;
  int file_id = 1;
  // prevent range deletions from being dropped due to becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();

  // range del [0, 50) in L6 file, [50, 100) in L0 file, [100, 150) in memtable
  for (int i = 0; i < 3; i++) {
    if (i != 0) {
      db_->Flush(FlushOptions());
      if (i == 1) {
        MoveFilesToLevel(kNumLevels - 1);
      }
    }
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(50 * i), Key(50 * (i + 1))));
  }
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 1));

  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  // overlaps with L0 file but not memtable, so flush is skipped and file is
  // ingested into L0
  SequenceNumber last_seqno = dbfull()->GetLatestSequenceNumber();
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {60, 90}, {ValueType::kTypeValue, ValueType::kTypeValue},
      {{65, 70}, {70, 85}}, file_id++, write_global_seqno,
      verify_checksums_before_ingest, &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), ++last_seqno);
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 1));

  // overlaps with L6 file but not memtable or L0 file, so flush is skipped and
  // file is ingested into L5
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {10, 40}, {ValueType::kTypeValue, ValueType::kTypeValue},
      file_id++, write_global_seqno, verify_checksums_before_ingest,
      &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), ++last_seqno);
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 1));

  // overlaps with L5 file but not memtable or L0 file, so flush is skipped and
  // file is ingested into L4
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {}, {}, {{5, 15}}, file_id++, write_global_seqno,
      verify_checksums_before_ingest, &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), ++last_seqno);
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 1));

  // ingested file overlaps with memtable, so flush is triggered before the file
  // is ingested such that the ingested data is considered newest. So L0 file
  // count increases by two.
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {100, 140}, {ValueType::kTypeValue, ValueType::kTypeValue},
      file_id++, write_global_seqno, verify_checksums_before_ingest,
      &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), ++last_seqno);
  ASSERT_EQ(4, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(1, NumTableFilesAtLevel(options.num_levels - 1));

  // snapshot unneeded now that all range deletions are persisted
  db_->ReleaseSnapshot(snapshot);

  // overlaps with nothing, so places at bottom level and skips incrementing
  // seqnum.
  ASSERT_OK(GenerateAndAddExternalFile(
      options, {151, 175}, {ValueType::kTypeValue, ValueType::kTypeValue},
      {{160, 200}}, file_id++, write_global_seqno,
      verify_checksums_before_ingest, &true_data));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), last_seqno);
  ASSERT_EQ(4, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 2));
  ASSERT_EQ(2, NumTableFilesAtLevel(options.num_levels - 1));
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithBadBlockChecksum) {
  bool change_checksum_called = false;
  const auto& change_checksum = [&](void* arg) {
    if (!change_checksum_called) {
      char* buf = reinterpret_cast<char*>(arg);
      assert(nullptr != buf);
      buf[0] ^= 0x1;
      change_checksum_called = true;
    }
  };
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WriteRawBlock:TamperWithChecksum",
      change_checksum);
  SyncPoint::GetInstance()->EnableProcessing();
  int file_id = 0;
  bool write_global_seqno = std::get<0>(GetParam());
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  do {
    Options options = CurrentOptions();
    DestroyAndReopen(options);
    std::map<std::string, std::string> true_data;
    Status s = GenerateAndAddExternalFile(
        options, {1, 2, 3, 4, 5, 6}, ValueType::kTypeValue, file_id++,
        write_global_seqno, verify_checksums_before_ingest, &true_data);
    if (verify_checksums_before_ingest) {
      ASSERT_NOK(s);
    } else {
      ASSERT_OK(s);
    }
    change_checksum_called = false;
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_P(ExternalSSTFileBasicTest, IngestFileWithFirstByteTampered) {
  SyncPoint::GetInstance()->DisableProcessing();
  int file_id = 0;
  EnvOptions env_options;
  do {
    Options options = CurrentOptions();
    std::string file_path = sst_files_dir_ + ToString(file_id++);
    SstFileWriter sst_file_writer(env_options, options);
    Status s = sst_file_writer.Open(file_path);
    ASSERT_OK(s);
    for (int i = 0; i != 100; ++i) {
      std::string key = Key(i);
      std::string value = Key(i) + ToString(0);
      ASSERT_OK(sst_file_writer.Put(key, value));
    }
    ASSERT_OK(sst_file_writer.Finish());
    {
      // Get file size
      uint64_t file_size = 0;
      ASSERT_OK(env_->GetFileSize(file_path, &file_size));
      ASSERT_GT(file_size, 8);
      std::unique_ptr<RandomRWFile> rwfile;
      ASSERT_OK(env_->NewRandomRWFile(file_path, &rwfile, EnvOptions()));
      // Manually corrupt the file
      // We deterministically corrupt the first byte because we currently
      // cannot choose a random offset. The reason for this limitation is that
      // we do not checksum property block at present.
      const uint64_t offset = 0;
      char scratch[8] = {0};
      Slice buf;
      ASSERT_OK(rwfile->Read(offset, sizeof(scratch), &buf, scratch));
      scratch[0] ^= 0xff;  // flip one bit
      ASSERT_OK(rwfile->Write(offset, buf));
    }
    // Ingest file.
    IngestExternalFileOptions ifo;
    ifo.write_global_seqno = std::get<0>(GetParam());
    ifo.verify_checksums_before_ingest = std::get<1>(GetParam());
    s = db_->IngestExternalFile({file_path}, ifo);
    if (ifo.verify_checksums_before_ingest) {
      ASSERT_NOK(s);
    } else {
      ASSERT_OK(s);
    }
  } while (ChangeOptionsForFileIngestionTest());
}

TEST_P(ExternalSSTFileBasicTest, IngestExternalFileWithCorruptedPropsBlock) {
  bool verify_checksums_before_ingest = std::get<1>(GetParam());
  if (!verify_checksums_before_ingest) {
    return;
  }
  uint64_t props_block_offset = 0;
  size_t props_block_size = 0;
  const auto& get_props_block_offset = [&](void* arg) {
    props_block_offset = *reinterpret_cast<uint64_t*>(arg);
  };
  const auto& get_props_block_size = [&](void* arg) {
    props_block_size = *reinterpret_cast<uint64_t*>(arg);
  };
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockOffset",
      get_props_block_offset);
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockSize",
      get_props_block_size);
  SyncPoint::GetInstance()->EnableProcessing();
  int file_id = 0;
  Random64 rand(time(nullptr));
  do {
    std::string file_path = sst_files_dir_ + ToString(file_id++);
    Options options = CurrentOptions();
    SstFileWriter sst_file_writer(EnvOptions(), options);
    Status s = sst_file_writer.Open(file_path);
    ASSERT_OK(s);
    for (int i = 0; i != 100; ++i) {
      std::string key = Key(i);
      std::string value = Key(i) + ToString(0);
      ASSERT_OK(sst_file_writer.Put(key, value));
    }
    ASSERT_OK(sst_file_writer.Finish());

    {
      std::unique_ptr<RandomRWFile> rwfile;
      ASSERT_OK(env_->NewRandomRWFile(file_path, &rwfile, EnvOptions()));
      // Manually corrupt the file
      ASSERT_GT(props_block_size, 8);
      uint64_t offset =
          props_block_offset + rand.Next() % (props_block_size - 8);
      char scratch[8] = {0};
      Slice buf;
      ASSERT_OK(rwfile->Read(offset, sizeof(scratch), &buf, scratch));
      scratch[0] ^= 0xff;  // flip one bit
      ASSERT_OK(rwfile->Write(offset, buf));
    }

    // Ingest file.
    IngestExternalFileOptions ifo;
    ifo.write_global_seqno = std::get<0>(GetParam());
    ifo.verify_checksums_before_ingest = true;
    s = db_->IngestExternalFile({file_path}, ifo);
    ASSERT_NOK(s);
  } while (ChangeOptionsForFileIngestionTest());
}

INSTANTIATE_TEST_CASE_P(ExternalSSTFileBasicTest, ExternalSSTFileBasicTest,
                        testing::Values(std::make_tuple(true, true),
                                        std::make_tuple(true, false),
                                        std::make_tuple(false, true),
                                        std::make_tuple(false, false)));

#endif  // ROCKSDB_LITE

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
