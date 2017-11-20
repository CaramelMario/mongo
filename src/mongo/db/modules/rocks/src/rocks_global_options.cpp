/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/base/status.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/constraints.h"

#include "rocks_global_options.h"

namespace mongo {

    RocksGlobalOptions rocksGlobalOptions;

    Status RocksGlobalOptions::add(moe::OptionSection* options) {
        moe::OptionSection rocksOptions("RocksDB options");

        rocksOptions.addOptionChaining("storage.rocksdb.cacheSizeGB", "rocksdbCacheSizeGB",
                                       moe::Int,
                                       "maximum amount of memory to allocate for cache; "
                                       "defaults to 30%% of physical RAM").validRange(1, 10000);
        rocksOptions.addOptionChaining("storage.rocksdb.compression", "rocksdbCompression",
                                       moe::String,
                                       "block compression algorithm for collection data "
                                       "[none|snappy|zlib|lz4|lz4hc]")
            .format("(:?none)|(:?snappy)|(:?zlib)|(:?lz4)|(:?lz4hc)", "(none/snappy/zlib/lz4/lz4hc)")
            .setDefault(moe::Value(std::string("snappy")));
        rocksOptions
            .addOptionChaining(
                 "storage.rocksdb.maxWriteMBPerSec", "rocksdbMaxWriteMBPerSec", moe::Int,
                 "Maximum speed that RocksDB will write to storage. Reducing this can "
                 "help reduce read latency spikes during compactions. However, reducing this "
                 "below a certain point might slow down writes. Defaults to 1GB/sec")
            .validRange(1, 1024)
            .setDefault(moe::Value(1024));
        rocksOptions.addOptionChaining("storage.rocksdb.configString", "rocksdbConfigString",
                                       moe::String,
                                       "RocksDB storage engine custom "
                                       "configuration settings").hidden();
        rocksOptions.addOptionChaining("storage.rocksdb.crashSafeCounters",
                                       "rocksdbCrashSafeCounters", moe::Bool,
                                       "If true, numRecord and dataSize counter will be consistent "
                                       "even after power failure. If false, numRecord and dataSize "
                                       "might be a bit inconsistent after power failure, but "
                                       "should be correct under normal conditions. Setting this to "
                                       "true will make database inserts a bit slower.")
            .setDefault(moe::Value(false))
            .hidden();

        rocksOptions
            .addOptionChaining("storage.rocksdb.counters",
                               "rocksdbCounters",
                               moe::Bool,
                               "If true, we will turn on RocksDB's advanced counters")
            .setDefault(moe::Value(false));
        rocksOptions
            .addOptionChaining("storage.rocksdb.singleDeleteIndex",
                               "rocksdbSingleDeleteIndex", moe::Bool,
                               "This is still experimental. "
                               "Use this only if you know what you're doing")
            .setDefault(moe::Value(false));

        // rocks add

        rocksOptions
                .addOptionChaining("storage.rocksdb.targetFileSizeMultiplier",
                                   "rocksdbTargetFileSizeMultiplier",
                                   moe::Int,
                                   "Rocksdb target file size multiplier. (default 1 for rocksdb, 2 for terarkdb)")
                .validRange(1, 100);

        rocksOptions
                .addOptionChaining("storage.rocksdb.numLevels",
                                   "rocksdbNumLevels",
                                   moe::Int,
                                   "Rocksdb max level number.")
                .validRange(2, 20)
                .setDefault(moe::Value(7));

        rocksOptions
                .addOptionChaining("storage.rocksdb.targetFileSizeBase",
                                   "rocksdbTargetFileSizeBase",
                                   moe::UnsignedLongLong,
                                   "Rocksdb target file size base. (default 64M for rocksdb, 512M for terarkdb)");

        rocksOptions
                .addOptionChaining("storage.rocksdb.separateColumnFamily",
                                   "rocksdbSeparateColumnFamily",
                                   moe::Bool,
                                   "Rocksdb separate column family")
                .setDefault(moe::Value(false));

        rocksOptions
                .addOptionChaining("storage.rocksdb.oplogChunkWiseDelete",
                                   "rocksdbOplogChunkWiseDelete",
                                   moe::Bool,
                                   "Rocksdb oplog chunk wise delete")
                .setDefault(moe::Value(false));

        // terark begin

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.enabled",
                                   "terarkEnable",
                                   moe::Bool,
                                   "Terark enable.")
                .setDefault(moe::Value(false))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.indexNestLevel",
                                   "terarkIndexNestLevel",
                                   moe::Int,
                                   "Index nest level.")
                .validRange(1, 10)
                .setDefault(moe::Value(3))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.indexTempLevel",
                                   "terarkIndexTempLevel",
                                   moe::Int,
                                   "Index temporary file level. "
                                   "Case -1, do not use temp file for index building. "
                                   "case  0, only use temp file for large index build, be smart. "
                                   "case  1,2,3 only use temp file for large index build, temp file 1 < 2 < 3, memory 1 > 2 > 3. "
                                   "case  4, always use temp file for all index build."
                                   )
                .validRange(-1, 4)
                .setDefault(moe::Value(4))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.checksumLevel",
                                   "terarkChecksumLevel",
                                   moe::Int,
                                   "Case 0, check sum nothing. "
                                   "case 1, check sum meta data and index, check on file load. "
                                   "case 2, check sum all data, not check on file load, check on record read. "
                                   "case 3, check sum all data with one checksum value, not checksum each record, "
                                   "if checksum doesn't match, load will fail")
                .validRange(0, 3)
                .setDefault(moe::Value(1));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.minPreadLen",
                                   "terarkMinPreadLen",
                                   moe::Int,
                                   "Case minPreadLen < 0, do not use pread. "
                                   "case minPreadLen = 0, always use pread. "
                                   "case minPreadLen > 0, use pread if BlobStore avg record len > minPreadLen")
                .setDefault(moe::Value(16));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.cacheShards",
                                   "terarkCacheShards",
                                   moe::Int,
                                   "Reduce lock competition. ")
                .setDefault(moe::Value(17));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.entropyAlgo",
                                   "terarkEntropyAlgo",
                                   moe::String,
                                   "Entropy algo. "
                                   "[none|huffman|FSE]")
                .format("(:?none)|(:?huffman)|(:?FSE)", "(none/huffman/FSE)")
                .setDefault(moe::Value(std::string("none")))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.terarkZipMinLevel",
                                   "terarkZipMinLevel",
                                   moe::Int,
                                   "When value < 0, only last level uses terarkZip. "
                                   "This is equivalent to when terarkZipMinLevel == num_levels-1. "
                                   "In other scenarios , use terarkZip when curlevel >= terarkZipMinLevel. "
                                   "This includes two special cases: "
                                   "(1) when value == 0, all levels use terarkZip; "
                                   "(2) when value >= num levels, all levels use fallback TableFactory. "
                                   "It shown that terarkZipMinLevel = 0 is the best choice. "
                                   "If mixed with rocksdb's native SST, "
                                   "those SSTs may use too much memory & SSD, "
                                   "which degrades the performance.")
                .setDefault(moe::Value(0));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.useSuffixArrayLocalMatch",
                                   "terarkUseSuffixArrayLocalMatch",
                                   moe::Bool,
                                   "Use suffix array local match.")
                .setDefault(moe::Value(false))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.warmUpIndexOnOpen",
                                   "terarkWarmUpIndexOnOpen",
                                   moe::Bool,
                                   "Warm up index on open.")
                .setDefault(moe::Value(true))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.warmUpValueOnOpen",
                                   "terarkWarmUpValueOnOpen",
                                   moe::Bool,
                                   "Warm up value on open.")
                .setDefault(moe::Value(false))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.estimateCompressionRatio",
                                   "terarkEstimateCompressionRatio",
                                   moe::Double,
                                   "To let rocksdb compaction algo know the estimate SST file size")
                .setDefault(moe::Value(0.2))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.sampleRatio",
                                   "terarkSampleRatio",
                                   moe::Double,
                                   "The global dictionary size over all value size")
                .setDefault(moe::Value(0.03))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.localTempDir",
                                   "terarkLocalTempDir",
                                   moe::String,
                                   "TerarkZipTable needs to create temp files during compression")
                .setDefault(moe::Value(std::string("/tmp")));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.cleanTempDir",
                                   "terarkCleanTempDir",
                                   moe::Bool,
                                   "clean temp dir on open.")
                .setDefault(moe::Value(false));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.indexType",
                                   "terarkIndexType",
                                   moe::String,
                                   "Index rank select type. ")
                .setDefault(moe::Value(std::string("IL_256")))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.initialWriteBufferSize",
                                   "terarkInitialWriteBufferSize",
                                   moe::UnsignedLongLong,
                                   "WriteBufferSize on initial sync (default 1G)")
                .setDefault(moe::Value(1ull << 30))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.softZipWorkingMemLimit",
                                   "terarkSoftMemLimit",
                                   moe::UnsignedLongLong,
                                   "Zip working memory soft limit (default 16G)")
                .setDefault(moe::Value(16ull << 30));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.hardZipWorkingMemLimit",
                                   "terarkHardMemLimit",
                                   moe::UnsignedLongLong,
                                   "Zip working memory hard limit (default 32G)")
                .setDefault(moe::Value(32ull << 30));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.smallTaskMemory",
                                   "terarkSmallTaskMemory",
                                   moe::UnsignedLongLong,
                                   "Small task memory size (default 1.2G)")
                .setDefault(moe::Value(1200ull << 20))
                .hidden();


        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.cacheCapacityBytes",
                                   "terarkCacheCapacityBytes",
                                   moe::UnsignedLongLong,
                                   "Non-zero implies direct io read. ")
                .setDefault(moe::Value(0ull));

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.indexCacheRatio",
                                   "terarkIndexCacheRatio",
                                   moe::Double,
                                   "Index cache ratio, should be a small value, typically 0.001. "
                                   "default is to disable indexCache, because the improvement. "
                                   "is about only 10% when set to 0.001")
                .setDefault(moe::Value(0.0))
                .hidden();

        rocksOptions
                .addOptionChaining("storage.rocksdb.terarkdb.zipThreads",
                                   "terarkZipThreads",
                                   moe::Int,
                                   "Zip threads for value compression, "
                                   "if greater than CPU count, use CPU count (default 8), ")
                .validRange(1, 64)
                .setDefault(moe::Value(8));

        // terark end

        return options->addSection(rocksOptions);
    }

    Status RocksGlobalOptions::store(const moe::Environment& params,
                                     const std::vector<std::string>& args) {
        if (params.count("storage.rocksdb.cacheSizeGB")) {
            rocksGlobalOptions.cacheSizeGB = params["storage.rocksdb.cacheSizeGB"].as<int>();
            log() << "Block Cache Size GB: " << rocksGlobalOptions.cacheSizeGB;
        }
        if (params.count("storage.rocksdb.compression")) {
            rocksGlobalOptions.compression =
                params["storage.rocksdb.compression"].as<std::string>();
            log() << "Compression: " << rocksGlobalOptions.compression;
        }
        if (params.count("storage.rocksdb.maxWriteMBPerSec")) {
            rocksGlobalOptions.maxWriteMBPerSec =
                params["storage.rocksdb.maxWriteMBPerSec"].as<int>();
            log() << "MaxWriteMBPerSec: " << rocksGlobalOptions.maxWriteMBPerSec;
        }
        if (params.count("storage.rocksdb.configString")) {
            rocksGlobalOptions.configString =
                params["storage.rocksdb.configString"].as<std::string>();
            log() << "Engine custom option: " << rocksGlobalOptions.configString;
        }
        if (params.count("storage.rocksdb.crashSafeCounters")) {
            rocksGlobalOptions.crashSafeCounters =
                params["storage.rocksdb.crashSafeCounters"].as<bool>();
            log() << "Crash safe counters: " << rocksGlobalOptions.crashSafeCounters;
        }
        if (params.count("storage.rocksdb.counters")) {
            rocksGlobalOptions.counters =
              params["storage.rocksdb.counters"].as<bool>();
            log() << "Counters: " << rocksGlobalOptions.counters;
        }
        if (params.count("storage.rocksdb.singleDeleteIndex")) {
            rocksGlobalOptions.singleDeleteIndex =
              params["storage.rocksdb.singleDeleteIndex"].as<bool>();
            log() << "Use SingleDelete in index: " << rocksGlobalOptions.singleDeleteIndex;
        }
        //rocks add
        if (params.count("storage.rocksdb.targetFileSizeMultiplier")) {
            rocksGlobalOptions.targetFileSizeMultiplier =
                    params["storage.rocksdb.targetFileSizeMultiplier"].as<int>();
            log() << "TargetFileSizeMultiplier: " << rocksGlobalOptions.targetFileSizeMultiplier;
        }
        if (params.count("storage.rocksdb.numLevels")) {
            rocksGlobalOptions.numLevels =
                    params["storage.rocksdb.numLevels"].as<int>();
            log() << "NumLevels: " << rocksGlobalOptions.numLevels;
        }
        if (params.count("storage.rocksdb.targetFileSizeBase")) {
            rocksGlobalOptions.targetFileSizeBase =
                    params["storage.rocksdb.targetFileSizeBase"].as<unsigned long long>();
            log() << "TargetFileSizeBase: " << rocksGlobalOptions.targetFileSizeBase;
        }
        if (params.count("storage.rocksdb.separateColumnFamily")) {
            rocksGlobalOptions.separateColumnFamily =
                    params["storage.rocksdb.separateColumnFamily"].as<bool>();
            log() << "SeparateColumnFamily: " << rocksGlobalOptions.separateColumnFamily;
        }
        if (params.count("storage.rocksdb.oplogChunkWiseDelete")) {
            rocksGlobalOptions.oplogChunkWiseDelete =
                    params["storage.rocksdb.oplogChunkWiseDelete"].as<bool>();
            log() << "OplogChunkWiseDelete: " << rocksGlobalOptions.oplogChunkWiseDelete;
        }
        if (params.count("replication.oplogSizeMB")) {
            rocksGlobalOptions.oplogSizeMB =
                    params["replication.oplogSizeMB"].as<int>();
            log() << "Replication.oplogSizeMB" << rocksGlobalOptions.oplogSizeMB;
        }
        //terark begin
        if (params.count("storage.rocksdb.terarkdb.enabled")) {
            rocksGlobalOptions.terarkEnable =
                    params["storage.rocksdb.terarkdb.enabled"].as<bool>();
            log() << "Terark terarkEnable: " << rocksGlobalOptions.terarkEnable;
        }
        if (rocksGlobalOptions.terarkEnable) {

        if (params.count("storage.rocksdb.terarkdb.indexNestLevel")) {
            rocksGlobalOptions.indexNestLevel =
                    params["storage.rocksdb.terarkdb.indexNestLevel"].as<int>();
            log() << "Terark IndexNestLevel: " << rocksGlobalOptions.indexNestLevel;
        }
        if (params.count("storage.rocksdb.terarkdb.indexTempLevel")) {
            rocksGlobalOptions.indexTempLevel =
                    params["storage.rocksdb.terarkdb.indexTempLevel"].as<int>();
            log() << "Terark IndexTempLevel: " << rocksGlobalOptions.indexTempLevel;
        }
        if (params.count("storage.rocksdb.terarkdb.checksumLevel")) {
            rocksGlobalOptions.checksumLevel =
                    params["storage.rocksdb.terarkdb.checksumLevel"].as<int>();
            log() << "Terark ChecksumLevel: " << rocksGlobalOptions.checksumLevel;
        }
        if (params.count("storage.rocksdb.terarkdb.minPreadLen")) {
            rocksGlobalOptions.minPreadLen =
                    params["storage.rocksdb.terarkdb.minPreadLen"].as<int>();
            log() << "Terark MinPreadLen: " << rocksGlobalOptions.minPreadLen;
        }
        if (params.count("storage.rocksdb.terarkdb.cacheShards")) {
            rocksGlobalOptions.cacheShards =
                    params["storage.rocksdb.terarkdb.cacheShards"].as<int>();
            log() << "Terark CacheShards: " << rocksGlobalOptions.cacheShards;
        }
        if (params.count("storage.rocksdb.terarkdb.entropyAlgo")) {
            rocksGlobalOptions.entropyAlgo =
                    params["storage.rocksdb.terarkdb.entropyAlgo"].as<std::string>();
            log() << "Terark EntropyAlgo: " << rocksGlobalOptions.entropyAlgo;
        }
        if (params.count("storage.rocksdb.terarkdb.terarkZipMinLevel")) {
            rocksGlobalOptions.terarkZipMinLevel =
                    params["storage.rocksdb.terarkdb.terarkZipMinLevel"].as<int>();
            log() << "Terark TerarkZipMinLevel: " << rocksGlobalOptions.terarkZipMinLevel;
        }
        if (params.count("storage.rocksdb.terarkdb.useSuffixArrayLocalMatch")) {
            rocksGlobalOptions.useSuffixArrayLocalMatch =
                    params["storage.rocksdb.terarkdb.useSuffixArrayLocalMatch"].as<bool>();
            log() << "Terark UseSuffixArrayLocalMatch: " << rocksGlobalOptions.useSuffixArrayLocalMatch;
        }
        if (params.count("storage.rocksdb.terarkdb.warmUpIndexOnOpen")) {
            rocksGlobalOptions.warmUpIndexOnOpen =
                    params["storage.rocksdb.terarkdb.warmUpIndexOnOpen"].as<bool>();
            log() << "Terark WarmUpIndexOnOpen: " << rocksGlobalOptions.warmUpIndexOnOpen;
        }
        if (params.count("storage.rocksdb.terarkdb.warmUpValueOnOpen")) {
            rocksGlobalOptions.warmUpValueOnOpen =
                    params["storage.rocksdb.terarkdb.warmUpValueOnOpen"].as<bool>();
            log() << "Terark WarmUpValueOnOpen: " << rocksGlobalOptions.warmUpValueOnOpen;
        }
        if (params.count("storage.rocksdb.terarkdb.estimateCompressionRatio")) {
            rocksGlobalOptions.estimateCompressionRatio =
                    params["storage.rocksdb.terarkdb.estimateCompressionRatio"].as<double>();
            log() << "Terark EstimateCompressionRatio: " << rocksGlobalOptions.estimateCompressionRatio;
        }
        if (params.count("storage.rocksdb.terarkdb.sampleRatio")) {
            rocksGlobalOptions.sampleRatio =
                    params["storage.rocksdb.terarkdb.sampleRatio"].as<double>();
            log() << "Terark SampleRatio: " << rocksGlobalOptions.sampleRatio;
        }
        if (params.count("storage.rocksdb.terarkdb.localTempDir")) {
            rocksGlobalOptions.localTempDir =
                    params["storage.rocksdb.terarkdb.localTempDir"].as<std::string>();
            log() << "Terark LocalTempDir: " << rocksGlobalOptions.localTempDir;
        }
        if (params.count("storage.rocksdb.terarkdb.cleanTempDir")) {
            rocksGlobalOptions.cleanTempDir =
                    params["storage.rocksdb.terarkdb.cleanTempDir"].as<bool>();
            log() << "Terark CleanTempDir: " << rocksGlobalOptions.cleanTempDir;
        }
        if (params.count("storage.rocksdb.terarkdb.indexType")) {
            rocksGlobalOptions.indexType =
                    params["storage.rocksdb.terarkdb.indexType"].as<std::string>();
            log() << "Terark IndexType: " << rocksGlobalOptions.indexType;
        }
        if (params.count("storage.rocksdb.terarkdb.initialWriteBufferSize")) {
            rocksGlobalOptions.initialWriteBufferSize =
                    params["storage.rocksdb.terarkdb.initialWriteBufferSize"].as<unsigned long long>();
            log() << "Terark InitialWriteBufferSize: " << rocksGlobalOptions.initialWriteBufferSize;
        }
        if (params.count("storage.rocksdb.terarkdb.softZipWorkingMemLimit")) {
            rocksGlobalOptions.softZipWorkingMemLimit =
                    params["storage.rocksdb.terarkdb.softZipWorkingMemLimit"].as<unsigned long long>();
            log() << "Terark SoftZipWorkingMemLimit: " << rocksGlobalOptions.softZipWorkingMemLimit;
        }
        if (params.count("storage.rocksdb.terarkdb.hardZipWorkingMemLimit")) {
            rocksGlobalOptions.hardZipWorkingMemLimit =
                    params["storage.rocksdb.terarkdb.hardZipWorkingMemLimit"].as<unsigned long long>();
            log() << "Terark HardZipWorkingMemLimit: " << rocksGlobalOptions.hardZipWorkingMemLimit;
        }
        if (params.count("storage.rocksdb.terarkdb.smallTaskMemory")) {
            rocksGlobalOptions.smallTaskMemory =
                    params["storage.rocksdb.terarkdb.smallTaskMemory"].as<unsigned long long>();
            log() << "Terark SmallTaskMemory: " << rocksGlobalOptions.smallTaskMemory;
        }
        if (params.count("storage.rocksdb.terarkdb.cacheCapacityBytes")) {
            rocksGlobalOptions.cacheCapacityBytes =
                    params["storage.rocksdb.terarkdb.cacheCapacityBytes"].as<unsigned long long>();
            log() << "Terark CacheCapacityBytes: " << rocksGlobalOptions.cacheCapacityBytes;
        }
        if (params.count("storage.rocksdb.terarkdb.indexCacheRatio")) {
            rocksGlobalOptions.indexCacheRatio =
                    params["storage.rocksdb.terarkdb.indexCacheRatio"].as<double>();
            log() << "Terark IndexCacheRatio: " << rocksGlobalOptions.indexCacheRatio;
        }
        if (params.count("storage.rocksdb.terarkdb.zipThreads")) {
            rocksGlobalOptions.terarkZipThreads =
                    params["storage.rocksdb.terarkdb.zipThreads"].as<int>();
            log() << "Terark ZipThreads: " << rocksGlobalOptions.terarkZipThreads;
        }

        } // if (rocksGlobalOptions.terarkEnable)
        //terark end

        return Status::OK();
    }

}  // namespace mongo
