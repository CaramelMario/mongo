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

#pragma once

#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

    namespace moe = mongo::optionenvironment;

    class RocksGlobalOptions {
    public:
        RocksGlobalOptions()
            : cacheSizeGB(0),
              maxWriteMBPerSec(1024),
              compression("snappy"),
              crashSafeCounters(false),
              counters(false),
              singleDeleteIndex(false),
              //rocks add
              targetFileSizeMultiplier(0),
              numLevels(7),
              targetFileSizeBase(0),
              oplogChunkWiseDelete(false),
              separateColumnFamily(false),
              enableAutoCompactOplog(true),
              //terark begin
              terarkEnable(true),
              indexNestLevel(3),
              indexTempLevel(4),
              checksumLevel(1),
              minPreadLen(16),
              cacheShards(17),
              entropyAlgo("none"),
              terarkZipMinLevel(0),
              useSuffixArrayLocalMatch(false),
              warmUpIndexOnOpen(true),
              warmUpValueOnOpen(false),
              cleanTempDir(false),
              estimateCompressionRatio(0.2),
              sampleRatio(0.03),
              localTempDir("/tmp"),
              indexType("IL_256"),
              initialWriteBufferSize(1ull << 30),
              softZipWorkingMemLimit(16ull << 30),
              hardZipWorkingMemLimit(32ull << 30),
              smallTaskMemory(1200ull << 20),
              cacheCapacityBytes(0),
              indexCacheRatio(0.0),
              terarkZipThreads(8),
              oplogSizeMB(256)
              //terark end
        {}

        Status add(moe::OptionSection* options);
        Status store(const moe::Environment& params, const std::vector<std::string>& args);

        size_t cacheSizeGB;
        int maxWriteMBPerSec;

        std::string compression;
        std::string configString;

        bool crashSafeCounters;
        bool counters;
        bool singleDeleteIndex;

        int targetFileSizeMultiplier;
        int numLevels;
        unsigned long long targetFileSizeBase;
        bool oplogChunkWiseDelete;
        bool separateColumnFamily;
        bool enableAutoCompactOplog;

        //terark begin
        bool terarkEnable;
        int indexNestLevel;
        int indexTempLevel;
        int checksumLevel;
        int minPreadLen;
        int cacheShards;
        std::string entropyAlgo;
        int terarkZipMinLevel;
        bool useSuffixArrayLocalMatch;
        bool warmUpIndexOnOpen;
        bool warmUpValueOnOpen;
        bool cleanTempDir;

        double estimateCompressionRatio;
        double sampleRatio;
        std::string localTempDir;
        std::string indexType;

        unsigned long long initialWriteBufferSize;
        unsigned long long softZipWorkingMemLimit;
        unsigned long long hardZipWorkingMemLimit;
        unsigned long long smallTaskMemory;
        unsigned long long cacheCapacityBytes;
        double indexCacheRatio;
        int terarkZipThreads;
        int oplogSizeMB;
        //terark end
    };

    extern RocksGlobalOptions rocksGlobalOptions;
}
