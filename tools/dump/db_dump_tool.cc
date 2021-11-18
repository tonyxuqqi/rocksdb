//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include <cinttypes>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/db_dump_tool.h"
#include "rocksdb/env.h"
#include "util/coding.h"
#include <string>

namespace rocksdb {

bool DbDumpTool::Run(const DumpOptions& dump_options,
                     rocksdb::Options options) {
  rocksdb::DB* dbptr;
  rocksdb::Status status;
  std::unique_ptr<rocksdb::WritableFile> dumpfile;
  char hostname[1024];
  int64_t timesec = 0;
  std::string abspath;
  char json[4096];

  static const char* magicstr = "ROCKDUMP";
  static const char versionstr[8] = {0, 0, 0, 0, 0, 0, 0, 1};

  rocksdb::Env* env = rocksdb::Env::Default();

  // Open the database
  options.create_if_missing = false;
 
  std::vector<std::string> cfs;
  DB::ListColumnFamilies(DBOptions(), dump_options.db_path, &cfs);
  std::vector<ColumnFamilyDescriptor> cfds;
  std::vector<ColumnFamilyHandle*> handles;
  for(size_t i = 0; i < cfs.size(); i++) {
     cfds.push_back(ColumnFamilyDescriptor(cfs[i], ColumnFamilyOptions()));
  }
  status = rocksdb::DB::OpenForReadOnly(options, dump_options.db_path, cfds, &handles, &dbptr, false);
  if (!status.ok()) {
    std::cerr << "Unable to open database '" << dump_options.db_path
              << "' for reading: " << status.ToString() << std::endl;
    return false;
  }

  const std::unique_ptr<rocksdb::DB> db(dbptr);

  status = env->NewWritableFile(dump_options.dump_location, &dumpfile,
                                rocksdb::EnvOptions());
  if (!status.ok()) {
    std::cerr << "Unable to open dump file '" << dump_options.dump_location
              << "' for writing: " << status.ToString() << std::endl;
    return false;
  }

  rocksdb::Slice magicslice(magicstr, 8);
  status = dumpfile->Append(magicslice);
  if (!status.ok()) {
    std::cerr << "Append failed: " << status.ToString() << std::endl;
    return false;
  }

  rocksdb::Slice versionslice(versionstr, 8);
  status = dumpfile->Append(versionslice);
  if (!status.ok()) {
    std::cerr << "Append failed: " << status.ToString() << std::endl;
    return false;
  }

  if (dump_options.anonymous) {
    snprintf(json, sizeof(json), "{}");
  } else {
    status = env->GetHostName(hostname, sizeof(hostname));
    status = env->GetCurrentTime(&timesec);
    status = env->GetAbsolutePath(dump_options.db_path, &abspath);
    snprintf(json, sizeof(json),
             "{ \"database-path\": \"%s\", \"hostname\": \"%s\", "
             "\"creation-time\": %" PRIi64 " }",
             abspath.c_str(), hostname, timesec);
  }

  rocksdb::Slice infoslice(json, strlen(json));
  char infosize[4];
  rocksdb::EncodeFixed32(infosize, (uint32_t)infoslice.size());
  rocksdb::Slice infosizeslice(infosize, 4);
  status = dumpfile->Append(infosizeslice);
  if (!status.ok()) {
    std::cerr << "Append failed: " << status.ToString() << std::endl;
    return false;
  }
  status = dumpfile->Append(infoslice);
  if (!status.ok()) {
    std::cerr << "Append failed: " << status.ToString() << std::endl;
    return false;
  }

  std::vector<std::string> livefiles;
  uint64_t manifest_file_size;
  db->GetLiveFiles(livefiles, &manifest_file_size, false);
  for (size_t i = 0; i < livefiles.size(); i++) {
      std::cout << "file " << livefiles[i] << std::endl;
  }
  for(size_t i = 0; i < cfs.size(); i++) {
      if (cfs[i] == "raft") {
          delete handles[i];
          continue;
      }
      ColumnFamilyHandle* cf = handles[i];
      std::cout << "dumping cf " << cfs[i] << std::endl;
      ColumnFamilyMetaData meta;
      db->GetColumnFamilyMetaData(cf, &meta);
      std::vector<LevelMetaData>& levels = meta.levels;
      std::cout << "file count " << meta.file_count << "  level count " << levels.size();
      for(size_t l = 0; l < levels.size(); l++) {
          std::cout << "level " << levels[l].level <<  "total size " << levels[l].size << std::endl;
          for(size_t j = 0; j < levels[l].files.size(); j++) {
             std::cout << "sst name:" << levels[l].files[j].name << "  size " << levels[l].files[j].size << std::endl;
          }
      }
      uint64_t key_count = 0;
  const std::unique_ptr<rocksdb::Iterator> it(
      db->NewIterator(rocksdb::ReadOptions(), handles[i]));
	  for (it->SeekToFirst(); it->Valid(); it->Next()) {
      key_count++;
	    char keysize[4];
	    rocksdb::EncodeFixed32(keysize, (uint32_t)it->key().size());
	    rocksdb::Slice keysizeslice(keysize, 4);
	    status = dumpfile->Append(keysizeslice);
	    if (!status.ok()) {
	      std::cerr << "Append failed: " << status.ToString() << std::endl;
	      return false;
	    }
	    status = dumpfile->Append(it->key());
	    if (!status.ok()) {
	      std::cerr << "Append failed: " << status.ToString() << std::endl;
	      return false;
	    }

	    char valsize[4];
	    rocksdb::EncodeFixed32(valsize, (uint32_t)it->value().size());
	    rocksdb::Slice valsizeslice(valsize, 4);
	    status = dumpfile->Append(valsizeslice);
	    if (!status.ok()) {
	      std::cerr << "Append failed: " << status.ToString() << std::endl;
	      return false;
	    }
	    status = dumpfile->Append(it->value());
	    if (!status.ok()) {
	      std::cerr << "Append failed: " << status.ToString() << std::endl;
	      return false;
	    }
	  }
	  if (!it->status().ok()) {
	    std::cerr << "Database iteration failed: " << status.ToString()
		      << std::endl;
	    return false;
	  }
    delete handles[i];
	  std::cout << cfs[i] << " key count " << key_count << std::endl;
  }
  return true;
}

bool DbUndumpTool::Run(const UndumpOptions& undump_options,
                       rocksdb::Options options) {
  rocksdb::DB* dbptr;
  rocksdb::Status status;
  rocksdb::Env* env;
  std::unique_ptr<rocksdb::SequentialFile> dumpfile;
  rocksdb::Slice slice;
  char scratch8[8];

  static const char* magicstr = "ROCKDUMP";
  static const char versionstr[8] = {0, 0, 0, 0, 0, 0, 0, 1};

  env = rocksdb::Env::Default();

  status = env->NewSequentialFile(undump_options.dump_location, &dumpfile,
                                  rocksdb::EnvOptions());
  if (!status.ok()) {
    std::cerr << "Unable to open dump file '" << undump_options.dump_location
              << "' for reading: " << status.ToString() << std::endl;
    return false;
  }

  status = dumpfile->Read(8, &slice, scratch8);
  if (!status.ok() || slice.size() != 8 ||
      memcmp(slice.data(), magicstr, 8) != 0) {
    std::cerr << "File '" << undump_options.dump_location
              << "' is not a recognizable dump file." << std::endl;
    return false;
  }

  status = dumpfile->Read(8, &slice, scratch8);
  if (!status.ok() || slice.size() != 8 ||
      memcmp(slice.data(), versionstr, 8) != 0) {
    std::cerr << "File '" << undump_options.dump_location
              << "' version not recognized." << std::endl;
    return false;
  }

  status = dumpfile->Read(4, &slice, scratch8);
  if (!status.ok() || slice.size() != 4) {
    std::cerr << "Unable to read info blob size." << std::endl;
    return false;
  }
  uint32_t infosize = rocksdb::DecodeFixed32(slice.data());
  status = dumpfile->Skip(infosize);
  if (!status.ok()) {
    std::cerr << "Unable to skip info blob: " << status.ToString() << std::endl;
    return false;
  }

  options.create_if_missing = true;
  status = rocksdb::DB::Open(options, undump_options.db_path, &dbptr);
  if (!status.ok()) {
    std::cerr << "Unable to open database '" << undump_options.db_path
              << "' for writing: " << status.ToString() << std::endl;
    return false;
  }

  const std::unique_ptr<rocksdb::DB> db(dbptr);

  uint32_t last_keysize = 64;
  size_t last_valsize = 1 << 20;
  std::unique_ptr<char[]> keyscratch(new char[last_keysize]);
  std::unique_ptr<char[]> valscratch(new char[last_valsize]);

  while (1) {
    uint32_t keysize, valsize;
    rocksdb::Slice keyslice;
    rocksdb::Slice valslice;

    status = dumpfile->Read(4, &slice, scratch8);
    if (!status.ok() || slice.size() != 4) break;
    keysize = rocksdb::DecodeFixed32(slice.data());
    if (keysize > last_keysize) {
      while (keysize > last_keysize) last_keysize *= 2;
      keyscratch = std::unique_ptr<char[]>(new char[last_keysize]);
    }

    status = dumpfile->Read(keysize, &keyslice, keyscratch.get());
    if (!status.ok() || keyslice.size() != keysize) {
      std::cerr << "Key read failure: "
                << (status.ok() ? "insufficient data" : status.ToString())
                << std::endl;
      return false;
    }

    status = dumpfile->Read(4, &slice, scratch8);
    if (!status.ok() || slice.size() != 4) {
      std::cerr << "Unable to read value size: "
                << (status.ok() ? "insufficient data" : status.ToString())
                << std::endl;
      return false;
    }
    valsize = rocksdb::DecodeFixed32(slice.data());
    if (valsize > last_valsize) {
      while (valsize > last_valsize) last_valsize *= 2;
      valscratch = std::unique_ptr<char[]>(new char[last_valsize]);
    }

    status = dumpfile->Read(valsize, &valslice, valscratch.get());
    if (!status.ok() || valslice.size() != valsize) {
      std::cerr << "Unable to read value: "
                << (status.ok() ? "insufficient data" : status.ToString())
                << std::endl;
      return false;
    }

    status = db->Put(rocksdb::WriteOptions(), keyslice, valslice);
    if (!status.ok()) {
      fprintf(stderr, "Unable to write database entry\n");
      return false;
    }
  }

  if (undump_options.compact_db) {
    status = db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
    if (!status.ok()) {
      fprintf(stderr,
              "Unable to compact the database after loading the dumped file\n");
      return false;
    }
  }
  return true;
}
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
