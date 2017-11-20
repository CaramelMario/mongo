/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
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
#include "mongo/util/quick_exit.h"

#include "rocks_engine.h"

#include <algorithm>
#include <mutex>

#include <boost/filesystem/operations.hpp>

#include <rocksdb/version.h>
#include <rocksdb/cache.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/table.h>
#include <rocksdb/convenience.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/utilities/checkpoint.h>

#include "mongo/db/client.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/concurrency/locker.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/storage/journal_listener.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/server_parameters.h"
#include "mongo/platform/endian.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/background.h"
#include "mongo/util/concurrency/idle_thread_block.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"

#include "rocks_counter_manager.h"
#include "rocks_global_options.h"
#include "rocks_record_store.h"
#include "rocks_recovery_unit.h"
#include "rocks_index.h"
#include "rocks_util.h"
#include <table/terark_zip_table.h>

#define ROCKS_TRACE log()

namespace terark {
    void DictZipBlobStore_setZipThreads(int zipThreads); // an hidden api from terark::DictZipBlobStore
}

namespace mongo {

    std::mutex oplogMutex;

    // collect oplog info by prefix
    class MongoRocksOplogPropertiesCollector : public rocksdb::TablePropertiesCollector {
    public:

        struct Info {
            int64_t timestamp;
            long long numRecords;
            long long dataSize;
        };

        rocksdb::Status AddUserKey(const rocksdb::Slice& key, const rocksdb::Slice& value,
                                   rocksdb::EntryType type, rocksdb::SequenceNumber /*seq*/,
                                   uint64_t /*file_size*/) override {
            if (type != rocksdb::kEntryPut || key.size() != 12) {
                //log() << "MongoRocksOplogPropertiesCollector : we got a key ,"
                //      << " type = " << int(type)
                //      << " size = " << key.size();
                dirty_ = true;
                return rocksdb::Status::OK();
            }
            dassert(key.size() == 12);
            uint32_t prefix;
            int64_t ts;
            memcpy(&prefix, key.data(), 4);
            memcpy(&ts, key.data() + 4, 8);

            if (data_.empty() || data_.back().first != prefix) {
                data_.emplace_back(prefix, Info{ts, (long long)1, (long long)value.size()});
            }
            else {
                Info& info = data_.back().second;
                info.timestamp = ts;
                ++info.numRecords;
                info.dataSize += value.size();
            }

            return rocksdb::Status::OK();
        }

        rocksdb::Status Finish(rocksdb::UserCollectedProperties* properties) override {
            std::string key = RocksEngine::kOplogCollectorPrefix;
            const size_t len = key.size();
            key.resize(len + 4);
            for (auto& item : data_) {
                memcpy(&key.front() + len, &item.first, 4);
                Info& info = item.second;
                info.numRecords = endian::nativeToBig(info.numRecords);
                info.dataSize = endian::nativeToBig(info.dataSize);
                properties->emplace(key, std::string((const char*)&info, 24));
            }
            if (dirty_) {
                properties->emplace(RocksEngine::kOplogCollectorDirty, std::string());
            }
            return rocksdb::Status::OK();
        }

        rocksdb::UserCollectedProperties GetReadableProperties() const override {
            rocksdb::UserCollectedProperties ret;
            for (auto& item : data_) {
                auto key = RocksEngine::kOplogCollectorPrefix +
                    rocksdb::Slice((const char*)&item.first, 4).ToString(true);
                Info info = item.second;
                info.numRecords = endian::nativeToBig(info.numRecords);
                info.dataSize = endian::nativeToBig(info.dataSize);
                auto value = rocksdb::Slice((const char*)&info, 24).ToString(true);
                ret.emplace(key, value);
            }
            if (dirty_) {
                ret.emplace(RocksEngine::kOplogCollectorDirty, std::string());
            }
            return ret;
        }

        const char* Name() const override {
            return "MongoRocksOplogPropertiesCollector";
        }

        static uint32_t Decode(const std::pair<const std::string, std::string>& pair, Info* info) {
            dassert(pair.first.size() == RocksEngine::kOplogCollectorPrefix.size() + 4);
            dassert(pair.second.size() == 24);
            uint32_t prefix;
            uint64_t timestamp;
            long long numRecords;
            long long dataSize;
            memcpy(&prefix, pair.first.data() + RocksEngine::kOplogCollectorPrefix.size(), 4);
            memcpy(&timestamp, pair.second.data(), 8);
            memcpy(&numRecords, pair.second.data() + 8, 8);
            memcpy(&dataSize, pair.second.data() + 16, 8);
            info->timestamp = endian::bigToNative(timestamp);
            info->numRecords = endian::bigToNative(numRecords);
            info->dataSize = endian::bigToNative(dataSize);
            return endian::bigToNative(prefix);
        }
        static long long DecodeTimestamp(const std::pair<const std::string, std::string>& pair) {
            dassert(pair.second.size() == 24);
            uint64_t timestamp;
            memcpy(&timestamp, pair.second.data(), 8);
            return endian::bigToNative(timestamp);
        }

    private:
        std::vector<std::pair<uint32_t, Info>> data_;
        bool dirty_ = false;
    };

    class MongoRocksOplogPropertiesCollectorFactory : public rocksdb::TablePropertiesCollectorFactory {
    public:
        rocksdb::TablePropertiesCollector* CreateTablePropertiesCollector(
          TablePropertiesCollectorFactory::Context context) override {
            return new MongoRocksOplogPropertiesCollector();
        }

        const char* Name() const override {
            return "MongoRocksOplogPropertiesCollectorFactory";
        }
    };

    class MongoRocksOplogFlushEventListener : public rocksdb::EventListener {
    public:
        MongoRocksOplogFlushEventListener(
            std::function<void(rocksdb::DB*, const rocksdb::FlushJobInfo&)> callback)
            : _callback(callback) {
        }
        void OnFlushCompleted(rocksdb::DB* db, const rocksdb::FlushJobInfo& flush_job_info) override {
            if (flush_job_info.cf_name == RocksEngine::kOplogCF && _callback) {
                _callback(db, flush_job_info);
            }
        }
    private:
        std::function<void(rocksdb::DB*, const rocksdb::FlushJobInfo&)> _callback;
    };

    class RocksEngine::RocksJournalFlusher : public BackgroundJob {
    public:
        explicit RocksJournalFlusher(RocksDurabilityManager* durabilityManager)
            : BackgroundJob(false /* deleteSelf */), _durabilityManager(durabilityManager) {}

        virtual std::string name() const { return "RocksJournalFlusher"; }

        virtual void run() {
            Client::initThread(name().c_str());

            LOG(1) << "starting " << name() << " thread";

            while (!_shuttingDown.load()) {
                try {
                    _durabilityManager->waitUntilDurable(false);
                } catch (const UserException& e) {
                    invariant(e.getCode() == ErrorCodes::ShutdownInProgress);
                }

                int ms = storageGlobalParams.journalCommitIntervalMs;
                if (!ms) {
                    ms = 100;
                }

                MONGO_IDLE_THREAD_BLOCK;
                sleepmillis(ms);
            }
            LOG(1) << "stopping " << name() << " thread";
        }

        void shutdown() {
            _shuttingDown.store(true);
            wait();
        }

    private:
        RocksDurabilityManager* _durabilityManager;  // not owned
        std::atomic<bool> _shuttingDown{false};      // NOLINT
    };

    namespace {
        // we encode prefixes in big endian because we want to quickly jump to the max prefix
        // (iter->SeekToLast())
        bool extractPrefix(const rocksdb::Slice& slice, uint32_t* prefix) {
            if (slice.size() < sizeof(uint32_t)) {
                return false;
            }
            *prefix = endian::bigToNative(*reinterpret_cast<const uint32_t*>(slice.data()));
            return true;
        }

        std::string encodePrefix(uint32_t prefix) {
            uint32_t bigEndianPrefix = endian::nativeToBig(prefix);
            return std::string(reinterpret_cast<const char*>(&bigEndianPrefix), sizeof(uint32_t));
        }

        class PrefixDeletingCompactionFilter : public rocksdb::CompactionFilter {
        public:
            explicit PrefixDeletingCompactionFilter(std::unordered_set<uint32_t> droppedPrefixes)
                : _droppedPrefixes(std::move(droppedPrefixes)),
                  _prefixCache(0),
                  _droppedCache(false) {}

            // filter is not called from multiple threads simultaneously
            virtual bool Filter(int level, const rocksdb::Slice& key,
                                const rocksdb::Slice& existing_value, std::string* new_value,
                                bool* value_changed) const {
                uint32_t prefix = 0;
                if (!extractPrefix(key, &prefix)) {
                    // this means there is a key in the database that's shorter than 4 bytes. this
                    // should never happen and this is a corruption. however, it's not compaction
                    // filter's job to report corruption, so we just silently continue
                    return false;
                }
                if (prefix == _prefixCache) {
                    return _droppedCache;
                }
                _prefixCache = prefix;
                _droppedCache = _droppedPrefixes.find(prefix) != _droppedPrefixes.end();
                return _droppedCache;
            }

            // IgnoreSnapshots is available since RocksDB 4.3
#if defined(ROCKSDB_MAJOR) && (ROCKSDB_MAJOR > 4 || (ROCKSDB_MAJOR == 4 && ROCKSDB_MINOR >= 3))
            virtual bool IgnoreSnapshots() const override { return true; }
#endif

            virtual const char* Name() const { return "PrefixDeletingCompactionFilter"; }

        private:
            std::unordered_set<uint32_t> _droppedPrefixes;
            mutable uint32_t _prefixCache;
            mutable bool _droppedCache;
        };

        class PrefixDeletingCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
        public:
            explicit
            PrefixDeletingCompactionFilterFactory(const RocksEngine* engine) : _engine(engine) {}

            virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
                const rocksdb::CompactionFilter::Context& context) override {
                auto droppedPrefixes = _engine->getDroppedPrefixes();
                if (droppedPrefixes.size() == 0) {
                    // no compaction filter needed
                    return std::unique_ptr<rocksdb::CompactionFilter>(nullptr);
                } else {
                    return std::unique_ptr<rocksdb::CompactionFilter>(
                        new PrefixDeletingCompactionFilter(std::move(droppedPrefixes)));
                }
            }

            virtual const char* Name() const override {
                return "PrefixDeletingCompactionFilterFactory";
            }

        private:
            const RocksEngine* _engine;
        };

        // ServerParameter to limit concurrency, to prevent thousands of threads running
        // concurrent searches and thus blocking the entire DB.
        class RocksTicketServerParameter : public ServerParameter {
            MONGO_DISALLOW_COPYING(RocksTicketServerParameter);

        public:
            RocksTicketServerParameter(TicketHolder* holder, const std::string& name)
                : ServerParameter(ServerParameterSet::getGlobal(), name, true, true), _holder(holder) {};
            virtual void append(OperationContext* txn, BSONObjBuilder& b, const std::string& name) {
                b.append(name, _holder->outof());
            }
            virtual Status set(const BSONElement& newValueElement) {
                if (!newValueElement.isNumber())
                    return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be a number");
                return _set(newValueElement.numberInt());
            }
            virtual Status setFromString(const std::string& str) {
                int num = 0;
                Status status = parseNumberFromString(str, &num);
                if (!status.isOK())
                    return status;
                return _set(num);
            }

        private:
            Status _set(int newNum) {
                if (newNum <= 0) {
                    return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be > 0");
                }

                return _holder->resize(newNum);
            }

            TicketHolder* _holder;
        };

        TicketHolder openWriteTransaction(128);
        RocksTicketServerParameter openWriteTransactionParam(&openWriteTransaction,
                                                        "rocksdbConcurrentWriteTransactions");

        TicketHolder openReadTransaction(128);
        RocksTicketServerParameter openReadTransactionParam(&openReadTransaction,
                                                       "rocksdbConcurrentReadTransactions");

    }  // anonymous namespace

    // first four bytes are the default prefix 0
    const std::string RocksEngine::kMetadataPrefix("\0\0\0\0metadata-", 12);
    const std::string RocksEngine::kDroppedPrefix("\0\0\0\0droppedprefix-", 18);

    const std::string RocksEngine::kMetaCF("metaCF");
    const std::string RocksEngine::kOplogCF("oplogCF");

    const std::string RocksEngine::kOplogCollectorPrefix("mongo-oplog.");
    const std::string RocksEngine::kOplogCollectorDirty("mongo-oplog-dirty");

    RocksEngine::RocksEngine(const std::string& path, bool durable, int formatVersion,
                             bool readOnly)
        : _path(path), _durable(durable), _formatVersion(formatVersion), _maxPrefix(0), _oplogThread(nullptr) {
        {  // create block cache
            uint64_t cacheSizeGB = rocksGlobalOptions.cacheSizeGB;
            if (cacheSizeGB == 0) {
                ProcessInfo pi;
                unsigned long long memSizeMB = pi.getMemSizeMB();
                if (memSizeMB > 0) {
                    // reserve 1GB for system and binaries, and use 30% of the rest
                    double cacheMB = (memSizeMB - 1024) * 0.3;
                    cacheSizeGB = static_cast<uint64_t>(cacheMB / 1024);
                }
                if (cacheSizeGB < 1) {
                    cacheSizeGB = 1;
                }
            }
            _block_cache = rocksdb::NewLRUCache(cacheSizeGB * 1024 * 1024 * 1024LL, 6);
        }
        _maxWriteMBPerSec = rocksGlobalOptions.maxWriteMBPerSec;
        _rateLimiter.reset(
            rocksdb::NewGenericRateLimiter(static_cast<int64_t>(_maxWriteMBPerSec) << 20, 10 * 1000));
        if (rocksGlobalOptions.counters) {
            _statistics = rocksdb::CreateDBStatistics();
        }
        // clean temp dir before open
        if (rocksGlobalOptions.cleanTempDir) {
            rocksdb::TerarkZipDeleteTempFiles(rocksGlobalOptions.localTempDir);
        }

        // open DB, make sure oplog-column-family will be created if
        rocksdb::Options options = _options();
        std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors = {
            rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, options)
        };

        auto& rgo = rocksGlobalOptions;
        rgo.enableAutoCompactOplog = !rgo.oplogChunkWiseDelete && (!rgo.terarkEnable || rgo.separateColumnFamily);
        if (!rgo.separateColumnFamily) {
            rgo.oplogChunkWiseDelete = false;
        }

        if (rgo.separateColumnFamily)
        {
            // meta cf config
            // block size 4k is better for read ~
            rocksdb::ColumnFamilyOptions metaOption;
            rocksdb::BlockBasedTableOptions bto;
            bto.block_size = 4ull << 10;
            metaOption.compression = rocksdb::kSnappyCompression;
            metaOption.table_factory =
                std::shared_ptr<rocksdb::TableFactory>(rocksdb::NewBlockBasedTableFactory(bto));
            cfDescriptors.emplace_back(kMetaCF, metaOption);
            options.max_total_wal_size =
                (options.max_write_buffer_number * options.write_buffer_size) + (1ULL << 30);
            if(rgo.oplogChunkWiseDelete)
            {
                // oplog cf config for chunk wise
                rocksdb::ColumnFamilyOptions oplogOption;
                rocksdb::BlockBasedTableOptions bto;
                bto.block_size = 32ull << 10;
                bto.no_block_cache = true;
                oplogOption.compaction_style = rocksdb::kCompactionStyleNone;
                oplogOption.write_buffer_size =
                    rocksGetWriteBufferSize((size_t)rocksGlobalOptions.oplogSizeMB << 20);
                oplogOption.target_file_size_base = oplogOption.write_buffer_size;
                oplogOption.disable_auto_compactions = true;
                oplogOption.compression = rocksdb::kSnappyCompression;
                oplogOption.table_factory =
                    std::shared_ptr<rocksdb::TableFactory>(rocksdb::NewBlockBasedTableFactory(bto));
                oplogOption.table_properties_collector_factories.emplace_back(
                    new MongoRocksOplogPropertiesCollectorFactory);
                oplogOption.level0_slowdown_writes_trigger = (1 << 30);
                oplogOption.level0_stop_writes_trigger = (1 << 30);
                oplogOption.max_write_buffer_number = 6;
                cfDescriptors.emplace_back(kOplogCF, oplogOption);
                options.listeners.emplace_back(
                    new MongoRocksOplogFlushEventListener([this](rocksdb::DB*, const rocksdb::FlushJobInfo&) {
                        _oplogReclaimOCV.notify_one();
                    }));
            }
            else
            {
                // oplog cf config
                rocksdb::ColumnFamilyOptions oplogOption;
                rocksdb::BlockBasedTableOptions bto;
                bto.block_size = 32ull << 10;
                bto.no_block_cache = true;
                oplogOption.compression = rocksdb::kSnappyCompression;
                oplogOption.table_factory =
                    std::shared_ptr<rocksdb::TableFactory>(rocksdb::NewBlockBasedTableFactory(bto));
                cfDescriptors.emplace_back(kOplogCF, oplogOption);
            }
        }
        else {
            _metaCFIndex = 0;
            _oplogCFIndex = 0;
        }
        rocksdb::DB* db;
        rocksdb::Status s = openDB(options, cfDescriptors, readOnly, &db);
        invariantRocksOK(s);
        _db.reset(db);

        _counterManager.reset(
            new RocksCounterManager(_db.get(), _cfHandles[_metaCFIndex], rocksGlobalOptions.crashSafeCounters));
        _compactionScheduler.reset(new RocksCompactionScheduler(_db.get()));

        // open iterator
        std::unique_ptr<rocksdb::Iterator> iter(_db->NewIterator(rocksdb::ReadOptions(), _cfHandles[_metaCFIndex]));
        std::unique_ptr<rocksdb::Iterator> iterData(_db->NewIterator(rocksdb::ReadOptions(),
                                                                     _cfHandles[_dataCFIndex]));
        std::unique_ptr<rocksdb::Iterator> iterOplog(_db->NewIterator(rocksdb::ReadOptions(),
                                                                      _cfHandles[_oplogCFIndex]));

        // find maxPrefix
        iterData->SeekToLast();
        if (iterData->Valid()) {
            // otherwise the DB is empty, so we just keep it at 0
            bool ok = extractPrefix(iterData->key(), &_maxPrefix);
            // this is DB corruption here
            invariant(ok);
        }
        if (rgo.separateColumnFamily) {
            iterOplog->SeekToLast();
            if(iterOplog->Valid()) {
                uint32_t oplogMaxPrefix = 0;
                bool ok = extractPrefix(iterOplog->key(), &oplogMaxPrefix);
                invariant(ok);
                _maxPrefix = std::max(oplogMaxPrefix, _maxPrefix);
            }
        }

        // load ident to prefix map. also update _maxPrefix if there's any prefix bigger than
        // current _maxPrefix
        {
            stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
            for (iter->Seek(kMetadataPrefix);
                 iter->Valid() && iter->key().starts_with(kMetadataPrefix); iter->Next()) {
                invariantRocksOK(iter->status());
                rocksdb::Slice ident(iter->key());
                ident.remove_prefix(kMetadataPrefix.size());
                // this could throw DBException, which then means DB corruption. We just let it fly
                // to the caller
                BSONObj identConfig(iter->value().data());
                BSONElement element = identConfig.getField("prefix");

                if (element.eoo() || !element.isNumber()) {
                    log() << "Mongo metadata in RocksDB database is corrupted.";
                    invariant(false);
                }
                uint32_t identPrefix = static_cast<uint32_t>(element.numberInt());

                _identMap[StringData(ident.data(), ident.size())] =
                    std::move(identConfig.getOwned());

                _maxPrefix = std::max(_maxPrefix, identPrefix);
            }
        }

        if (!rgo.oplogChunkWiseDelete) {
            // just to be extra sure. we need this if last collection is oplog -- in that case we
            // reserve prefix+1 for oplog key tracker
            ++_maxPrefix;
        }

        // load dropped prefixes
        if (rgo.terarkEnable) {
            rocksdb::WriteBatch wb;
            // we will use this iter to check if prefixes are still alive
            for (iter->Seek(kDroppedPrefix);
                 iter->Valid() && iter->key().starts_with(kDroppedPrefix); iter->Next()) {
                invariantRocksOK(iter->status());
                rocksdb::Slice prefix(iter->key());
                prefix.remove_prefix(kDroppedPrefix.size());
                bool isOplog = !iter->value().empty()
                               && BSONObj(iter->value().data()).getField("is_oplog").trueValue();
                auto iterSeek = isOplog ? iterOplog.get() : iterData.get();
                iterSeek->Seek(prefix);
                invariantRocksOK(iterSeek->status());
                if (iterSeek->Valid() && iterSeek->key().starts_with(prefix)) {
                    // prefix is still alive, let's instruct the compaction filter to clear it up
                    uint32_t int_prefix;
                    bool ok = extractPrefix(prefix, &int_prefix);
                    invariant(ok);
                    {
                        stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
                        _droppedPrefixes.insert(int_prefix);
                    }
                } else {
                    // prefix is no longer alive. let's remove the prefix from our dropped prefixes
                    // list
                    wb.Delete(_cfHandles[_metaCFIndex], iter->key());
                }
            }
            if (wb.Count() > 0) {
                auto s = _db->Write(rocksdb::WriteOptions(), &wb);
                invariantRocksOK(s);
            }
        } else {
            int dropped_count = 0;
            for (iter->Seek(kDroppedPrefix);
                 iter->Valid() && iter->key().starts_with(kDroppedPrefix); iter->Next()) {
                invariantRocksOK(iter->status());
                rocksdb::Slice prefix(iter->key());
                std::string prefixkey(prefix.ToString());
                prefix.remove_prefix(kDroppedPrefix.size());
                bool isOplog = !iter->value().empty()
                               && BSONObj(iter->value().data()).getField("is_oplog").trueValue();

                // let's instruct the compaction scheduler to compact dropped prefix
                ++dropped_count;
                uint32_t int_prefix;
                bool ok = extractPrefix(prefix, &int_prefix);
                invariant(ok);
                {
                    stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
                    _droppedPrefixes.insert(int_prefix);
                }
                LOG(1) << "compacting dropped prefix: " << prefix.ToString(true);
                auto s = _compactionScheduler->compactDroppedPrefix(
                            _cfHandles[isOplog ? _oplogCFIndex : _dataCFIndex],
                            prefix.ToString(),
                            [=] (bool opSucceeded) {
                                {
                                    stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
                                    _droppedPrefixes.erase(int_prefix);
                                }
                                if (opSucceeded) {
                                    rocksdb::WriteOptions syncOptions;
                                    syncOptions.sync = true;
                                    _db->Delete(syncOptions, _cfHandles[_metaCFIndex], prefixkey);
                                }
                            });
                if (!s.isOK()) {
                    log() << "failed to schedule compaction for prefix " << prefix.ToString(true);
                }
            }
            log() << dropped_count << " dropped prefixes need compaction";
        }
        iterOplog.reset();
        iterData.reset();
        iter.reset();

        // load oplog numRecords & dataSize . all table flush to sst when open
        {
            MongoRocksOplogPropertiesCollector::Info sstInfo;
            for (size_t iter_i = 0, finish_i = 1; iter_i < finish_i; ++iter_i) {
                rocksdb::TablePropertiesCollection prop;
                _db->GetPropertiesOfAllTables(_cfHandles[_oplogCFIndex], &prop);
                for (auto &pair : prop) {
                    auto user_prop = pair.second->user_collected_properties;
                    if (user_prop.find(RocksEngine::kOplogCollectorDirty) != user_prop.end()) {
                        log() << "Dirty oplog ... RocksRecordStore rollback oplog unclean shutdown ...";
                        _oplogInitializationMap.clear();
                        rocksdb::ColumnFamilyMetaData meta;
                        _db->GetColumnFamilyMetaData(_cfHandles[_oplogCFIndex], &meta);
                        std::vector<std::string> files;
                        files.reserve(meta.levels[0].files.size());
                        for (auto& file : meta.levels[0].files) {
                            files.emplace_back(file.name);
                        }
                        _db->CompactFiles(rocksdb::CompactionOptions(), _cfHandles[_oplogCFIndex], files, 0);
                        ++finish_i;
                        break;
                    }
                    auto &opPreifx = kOplogCollectorPrefix;
                    for (auto it = user_prop.lower_bound(opPreifx);
                        it != user_prop.end() && it->first.compare(0, opPreifx.size(), opPreifx) == 0;
                        ++it) {
                        uint32_t prefix = MongoRocksOplogPropertiesCollector::Decode(*it, &sstInfo);
                        auto ib = _oplogInitializationMap.emplace(prefix,
                                                                  OplogCollectionInit{sstInfo.numRecords,
                                                                                      sstInfo.dataSize});
                        if (!ib.second) {
                            OplogCollectionInit &init = ib.first->second;
                            init.numRecords += sstInfo.numRecords;
                            init.dataSize += sstInfo.dataSize;
                        }
                    }
                }
            }
        }

        _durabilityManager.reset(new RocksDurabilityManager(_db.get(), _durable));

        if (_durable) {
            _journalFlusher = stdx::make_unique<RocksJournalFlusher>(_durabilityManager.get());
            _journalFlusher->go();
        }

        Locker::setGlobalThrottling(&openReadTransaction, &openWriteTransaction);
        if (rocksGlobalOptions.oplogChunkWiseDelete) {
            _oplogThread = new std::thread(&RocksEngine::ReclaimOplog, this);
        }
    }

    RocksEngine::~RocksEngine() { cleanShutdown(); }

    rocksdb::Status RocksEngine::openDB(rocksdb::Options& options,
                                        std::vector<rocksdb::ColumnFamilyDescriptor>& descriptors,
                                        bool readOnly, rocksdb::DB** outdb) {
        // we save this key in default CF
        std::string SeparateTagKey("\0\0\0\0SeparateTag", 15);
        std::string kOplogChunkWiseDelete("OplogChunkWiseDelete");
        rocksdb::DB* db = nullptr;
        rocksdb::Status s;
        std::string value;
        if (readOnly) {
            s = rocksdb::DB::OpenForReadOnly(options, _path, descriptors, &_cfHandles, &db);
            if (!s.ok()) {
                error() << "Fail to open db: " << s.ToString();
                mongo::quickExit(1);
            }
            s = db->Get(rocksdb::ReadOptions(), SeparateTagKey, &value);
            if (rocksGlobalOptions.separateColumnFamily) {
                if (s.IsNotFound()) {
                    dassert(descriptors.size() == 3);
                    error() << "Internal error: Missing key SeparateTag";
                    mongo::quickExit(1);
                }
                dassert(descriptors[1].name == kMetaCF);
                dassert(descriptors[2].name == kOplogCF);
                if (!rocksGlobalOptions.oplogChunkWiseDelete != !(value == kOplogChunkWiseDelete)) {
                    error() << "Inconsistent Option, SeparateColumnFamily should be "
                            << (!rocksGlobalOptions.oplogChunkWiseDelete ? "true" : "false");
                    mongo::quickExit(1);
                }
            } else {
                if(s.ok() || _cfHandles.size() != 1) {
                    dassert(descriptors.size() == 1);
                    error() << "Inconsistent Option, SeparateColumnFamily should be true";
                    mongo::quickExit(1);
                }
            }
            *outdb = db;
            return rocksdb::Status::OK();
        } else {
            s = rocksdb::DB::Open(options, _path, descriptors, &_cfHandles, &db);
            if (s.ok()) {
                dassert(_cfHandles.size() == descriptors.size());
                s = db->Get(rocksdb::ReadOptions(), SeparateTagKey, &value);
                if(rocksGlobalOptions.separateColumnFamily) {
                    dassert(_cfHandles[_metaCFIndex]->GetName() == kMetaCF);
                    dassert(_cfHandles[_oplogCFIndex]->GetName() == kOplogCF);
                    if(s.IsNotFound()) {
                        error() << "Internal error: Missing key SeparateTag";
                        mongo::quickExit(1);
                    }
                    if (!rocksGlobalOptions.oplogChunkWiseDelete != !(value == kOplogChunkWiseDelete)) {
                        error() << "Inconsistent Option, SeparateColumnFamily should be "
                                << (!rocksGlobalOptions.oplogChunkWiseDelete ? "true" : "false");
                        mongo::quickExit(1);
                    }
                }
                else {
                    if(s.ok()) {
                        error() << "Internal error: Should not exist key SeparateTag";
                        mongo::quickExit(1);
                    }
                }
                *outdb = db;
                return rocksdb::Status::OK();
            } else {
                if (!rocksGlobalOptions.separateColumnFamily) {
                    return s;
                }
                std::vector<rocksdb::ColumnFamilyDescriptor> defaultDescriptor;
                defaultDescriptor.emplace_back(descriptors[0]);
                // create or open a singlg CF db ...
                s = rocksdb::DB::Open(options, _path, defaultDescriptor, &_cfHandles, &db);
                if (!s.ok()) {
                    // wot happend ?
                    error() << "Fail to open db: " << s.ToString();
                    mongo::quickExit(1);
                }
                // LatestSequenceNumber == 0 means DB is empty ...
                if (db->GetLatestSequenceNumber() != 0) {
                    s = db->Get(rocksdb::ReadOptions(), SeparateTagKey, &value);
                    if (s.ok()) {
                        // already put SeparateTagKey , but no metaCF or oplogCF ...
                        // this case we can't hold ...
                        // clean db dir if possible ...
                        error() << "Internal error: Found SeparateTag , but only default CF";
                        mongo::quickExit(1);
                    }
                    else
                    {
                        // missing SeparateTagKey ...
                        // it may works on single CF mode
                        error() << "Inconsistent Option, SeparateColumnFamily should be false";
                        mongo::quickExit(1);
                    }
                }
                db->Put(rocksdb::WriteOptions(), SeparateTagKey,
                        rocksGlobalOptions.oplogChunkWiseDelete ? kOplogChunkWiseDelete : "");
                // manually create metaCF, oplogCF
                rocksdb::ColumnFamilyHandle *metaCFH = nullptr, *oplogCFH = nullptr;
                dassert(descriptors.size() == 3);
                dassert(descriptors[1].name == kMetaCF);
                dassert(descriptors[2].name == kOplogCF);
                s = db->CreateColumnFamily(descriptors[1].options, kMetaCF, &metaCFH);
                dassert(s.ok());
                dassert(metaCFH->GetID() == _metaCFIndex);
                s = db->CreateColumnFamily(descriptors[2].options, kOplogCF, &oplogCFH);
                dassert(s.ok());
                dassert(oplogCFH->GetID() == _oplogCFIndex);
                _cfHandles.emplace_back(metaCFH);
                _cfHandles.emplace_back(oplogCFH);
                *outdb = db;
                return rocksdb::Status::OK();
            }
        }
    }

    void RocksEngine::appendGlobalStats(BSONObjBuilder& b) {
        BSONObjBuilder bb(b.subobjStart("concurrentTransactions"));
        {
            BSONObjBuilder bbb(bb.subobjStart("write"));
            bbb.append("out", openWriteTransaction.used());
            bbb.append("available", openWriteTransaction.available());
            bbb.append("totalTickets", openWriteTransaction.outof());
            bbb.done();
        }
        {
            BSONObjBuilder bbb(bb.subobjStart("read"));
            bbb.append("out", openReadTransaction.used());
            bbb.append("available", openReadTransaction.available());
            bbb.append("totalTickets", openReadTransaction.outof());
            bbb.done();
        }
        bb.done();
    }

    RecoveryUnit* RocksEngine::newRecoveryUnit() {
        return new RocksRecoveryUnit(&_transactionEngine, &_snapshotManager, _db.get(),
                                     _counterManager.get(), _compactionScheduler.get(),
                                     _durabilityManager.get(), _durable);
    }

    Status RocksEngine::createRecordStore(OperationContext* opCtx, StringData ns, StringData ident,
                                          const CollectionOptions& options) {
        BSONObjBuilder configBuilder;
        bool isOplog = NamespaceString::oplog(ns);
        auto s = _createIdent(ident, &configBuilder, isOplog);
        if (s.isOK() && isOplog && !rocksGlobalOptions.oplogChunkWiseDelete) {
            _oplogIdent = ident.toString();
            // oplog needs two prefixes, so we also reserve the next one
            uint64_t oplogTrackerPrefix = 0;
            {
                stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
                oplogTrackerPrefix = ++_maxPrefix;
            }
            // we also need to write out the new prefix to the database. this is just an
            // optimization
            std::string encodedPrefix(encodePrefix(oplogTrackerPrefix));
            s = rocksToMongoStatus(
                _db->Put(rocksdb::WriteOptions(), _cfHandles[_oplogCFIndex], encodedPrefix, rocksdb::Slice()));
        }
        return s;
    }

    std::unique_ptr<RecordStore> RocksEngine::getRecordStore(OperationContext* opCtx, StringData ns,
                                             StringData ident, const CollectionOptions& options) {
        bool isOplog = NamespaceString::oplog(ns);
        if (isOplog) {
            _oplogIdent = ident.toString();
        }

        auto config = _getIdentConfig(ident);
        std::string prefix = _extractPrefix(config);

        auto cfHandle = _cfHandles[isOplog ? _oplogCFIndex : _dataCFIndex];
        RocksRecordStore* rocksRecordStore;
        std::unique_ptr<RecordStore> recordStore(rocksRecordStore =
            options.capped
                ? new RocksRecordStore(
                      ns, ident, _db.get(), cfHandle, _counterManager.get(), _durabilityManager.get(),
                      _compactionScheduler.get(), prefix,
                      true, options.cappedSize ? options.cappedSize : 4096,  // default size
                      options.cappedMaxDocs ? options.cappedMaxDocs : -1)
                : new RocksRecordStore(ns, ident, _db.get(), cfHandle, _counterManager.get(),
                                       _durabilityManager.get(), _compactionScheduler.get(),
                                       prefix)
        );

        if (isOplog && rocksGlobalOptions.oplogChunkWiseDelete) {
            std::weak_ptr<RocksRecordStore> weakRocksRecordStore;
            auto proxy = CreateRocksRecordStoreProxy(rocksRecordStore, &weakRocksRecordStore);
            recordStore.release();
            recordStore.reset(proxy);
            stdx::lock_guard<stdx::mutex> lk(_oplogMapMutex);
            uint32_t numPrefix = endian::bigToNative(*(const uint32_t*)(prefix.data()));
            _oplogPrefixMap[numPrefix] = weakRocksRecordStore;
            // correct numRecords & dataSize
            auto find = _oplogInitializationMap.find(numPrefix);
            if (find != _oplogInitializationMap.end()) {
                auto& status = find->second;
                rocksRecordStore->updateStatsAfterRepair(opCtx, status.numRecords, status.dataSize);
                _oplogInitializationMap.erase(find);    // unused anymore
            }
        }
        {
            stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
            _identCollectionMap[ident] = rocksRecordStore;
        }
        return std::move(recordStore);
    }

    Status RocksEngine::createSortedDataInterface(OperationContext* opCtx, StringData ident,
                                                  const IndexDescriptor* desc) {
        BSONObjBuilder configBuilder;
        // let index add its own config things
        RocksIndexBase::generateConfig(&configBuilder, _formatVersion, desc->version());
        return _createIdent(ident, &configBuilder);
    }

    SortedDataInterface* RocksEngine::getSortedDataInterface(OperationContext* opCtx,
                                                             StringData ident,
                                                             const IndexDescriptor* desc) {

        auto config = _getIdentConfig(ident);
        std::string prefix = _extractPrefix(config);
        bool isOplog = config.getField("is_oplog").trueValue();
        auto cfHandle = _cfHandles[isOplog ? _oplogCFIndex : _dataCFIndex];

        RocksIndexBase* index;
        if (desc->unique()) {
            index = new RocksUniqueIndex(_db.get(), cfHandle, prefix, ident.toString(),
                                         Ordering::make(desc->keyPattern()), std::move(config),
                                         desc->parentNS(), desc->indexName(), desc->isPartial());
        } else {
            auto si = new RocksStandardIndex(_db.get(), cfHandle, prefix, ident.toString(),
                                             Ordering::make(desc->keyPattern()), std::move(config));
            if (rocksGlobalOptions.singleDeleteIndex) {
                si->enableSingleDelete();
            }
            index = si;
        }
        {
            stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
            _identIndexMap[ident] = index;
        }
        return index;
    }

    // cannot be rolled back
    Status RocksEngine::dropIdent(OperationContext* opCtx, StringData ident) {
        rocksdb::WriteBatch wb;
        wb.Delete(_cfHandles[_metaCFIndex], kMetadataPrefix + ident.toString());

        auto config = _getIdentConfig(ident);
        std::string prefix = _extractPrefix(config);
        bool isOplog = _oplogIdent == ident.toString() || config.getField("is_oplog").trueValue();

        // calculate which prefixes we need to drop
        std::vector<std::string> prefixesToDrop;
        prefixesToDrop.push_back(prefix);
        if (isOplog && rocksGlobalOptions.oplogChunkWiseDelete) {
            // if we're dropping oplog, we also need to drop keys from RocksOplogKeyTracker (they
            // are stored at prefix+1)
            prefixesToDrop.push_back(rocksGetNextPrefix(prefixesToDrop[0]));
        }

        // We record the fact that we're deleting this prefix. That way we ensure that the prefix is
        // always deleted
        for (const auto& prefix : prefixesToDrop) {
            wb.Put(_cfHandles[_metaCFIndex], kDroppedPrefix + prefix,
                   rocksdb::Slice(config.objdata(), config.objsize()));
        }

        // we need to make sure this is on disk before starting to delete data in compactions
        rocksdb::WriteOptions syncOptions;
        syncOptions.sync = true;
        auto s = _db->Write(syncOptions, &wb);
        if (!s.ok()) {
            return rocksToMongoStatus(s);
        }

        {
            stdx::lock_guard<stdx::mutex> lk(_oplogMapMutex);
            _oplogPrefixMap.erase(endian::bigToNative(*(const uint32_t*)(prefix.data())));
        }

        // remove from map
        {
            stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
            _identMap.erase(ident);
        }

        // instruct compaction filter to start deleting
        {
            stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
            for (const auto& prefix : prefixesToDrop) {
                uint32_t int_prefix;
                bool ok = extractPrefix(prefix, &int_prefix);
                invariant(ok);
                _droppedPrefixes.insert(int_prefix);
            }
        }

        // Suggest compaction for the prefixes that we need to drop, So that
        // we free space as fast as possible.
        for (auto& prefix : prefixesToDrop) {
            if (isOplog && rocksGlobalOptions.oplogChunkWiseDelete) {
                break;
            }
            if (rocksGlobalOptions.terarkEnable && !rocksGlobalOptions.separateColumnFamily) {
                break;
            }
            auto s = _compactionScheduler->compactDroppedPrefix(
                        _cfHandles[isOplog ? _oplogCFIndex : _dataCFIndex],
                        prefix,
                        [=] (bool opSucceeded) {
                            {
                                uint32_t int_prefix;
                                bool ok = extractPrefix(prefix, &int_prefix);
                                invariant(ok);
                                stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
                                _droppedPrefixes.erase(int_prefix);
                            }
                            if (opSucceeded) {
                                rocksdb::WriteOptions syncOptions;
                                syncOptions.sync = true;
                                _db->Delete(syncOptions, _cfHandles[_metaCFIndex], kDroppedPrefix + prefix);
                            }
                        });
            if (!s.isOK()) {
                log() << "failed to schedule compaction for prefix " << rocksdb::Slice(prefix).ToString(true);
            }
        }

        return Status::OK();
    }

    bool RocksEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
        stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
        return _identMap.find(ident) != _identMap.end();
    }

    std::vector<std::string> RocksEngine::getAllIdents(OperationContext* opCtx) const {
        std::vector<std::string> indents;
        for (auto& entry : _identMap) {
            indents.push_back(entry.first);
        }
        return indents;
    }

    void RocksEngine::cleanShutdown() {
        _shuttingDown = true;
        if (_oplogThread) {
            _oplogThread->join();
            delete _oplogThread;
        }
        if (_journalFlusher) {
            _journalFlusher->shutdown();
            _journalFlusher.reset();
        }
        _durabilityManager.reset();
        _snapshotManager.dropAllSnapshots();
        _counterManager->sync();
        _counterManager.reset();
        _compactionScheduler.reset();
        _db.reset();
    }

    long long RocksEngine::storageSize() const {
        if (!_db) {
            return 0;
        }
        long long size = 0;
        std::string strSize;
        if (_db->GetProperty(rocksdb::DB::Properties::kTotalSstFilesSize, &strSize)) {
            size += std::atol(strSize.c_str());
        }
        if (_db->GetProperty(rocksdb::DB::Properties::kSizeAllMemTables, &strSize)) {
            size += std::atol(strSize.c_str());
        }
        return size;
    }

    struct RocksOptionsBackup {
        bool disable_auto_compactions;
        int num_levels;
        int level0_file_num_compaction_trigger;
        int level0_slowdown_writes_trigger;
        int level0_stop_writes_trigger;
        int max_write_buffer_number;
        int max_background_compactions;
        int base_background_compactions;
        uint64_t write_buffer_size;
        uint64_t arena_block_size;
        uint64_t soft_pending_compaction_bytes_limit;
        uint64_t hard_pending_compaction_bytes_limit;
        uint64_t max_compaction_bytes;

        std::unordered_map<std::string, std::string> dumpDBOptions() {
            std::unordered_map<std::string, std::string> ret;
            StringBuilder sb;
            ret.emplace("max_background_compactions",
                        (sb.reset(32), sb << max_background_compactions, sb.str()));
            ret.emplace("base_background_compactions",
                        (sb.reset(32), sb << base_background_compactions, sb.str()));
            return ret;
        };

        std::unordered_map<std::string, std::string> dumpCFOptions() {
            std::unordered_map<std::string, std::string> ret;
            StringBuilder sb;
            ret.emplace("disable_auto_compactions",
                        disable_auto_compactions ? "1" : "0");
            ret.emplace("level0_file_num_compaction_trigger",
                        (sb.reset(32), sb << level0_file_num_compaction_trigger, sb.str()));
            ret.emplace("level0_slowdown_writes_trigger",
                        (sb.reset(32), sb << level0_slowdown_writes_trigger, sb.str()));
            ret.emplace("level0_stop_writes_trigger",
                        (sb.reset(32), sb << level0_stop_writes_trigger, sb.str()));
            ret.emplace("max_write_buffer_number",
                        (sb.reset(32), sb << max_write_buffer_number, sb.str()));
            ret.emplace("write_buffer_size",
                        (sb.reset(32), sb << write_buffer_size, sb.str()));
            ret.emplace("arena_block_size",
                        (sb.reset(32), sb << arena_block_size, sb.str()));
            ret.emplace("soft_pending_compaction_bytes_limit",
                        (sb.reset(32), sb << soft_pending_compaction_bytes_limit, sb.str()));
            ret.emplace("hard_pending_compaction_bytes_limit",
                        (sb.reset(32), sb << hard_pending_compaction_bytes_limit, sb.str()));
            ret.emplace("max_compaction_bytes",
                        (sb.reset(32), sb << max_compaction_bytes, sb.str()));
            return ret;
        };
    };

    void* RocksEngine::prepareInitialSync() {
        if (!rocksGlobalOptions.terarkEnable) {
            return nullptr;
        }
        auto options = _db->GetOptions();
        auto oldOptions = new RocksOptionsBackup{
            options.disable_auto_compactions,
            options.num_levels,
            options.level0_file_num_compaction_trigger,
            options.level0_slowdown_writes_trigger,
            options.level0_stop_writes_trigger,
            options.max_write_buffer_number,
            options.max_background_compactions,
            options.base_background_compactions,
            options.write_buffer_size,
            options.arena_block_size,
            options.soft_pending_compaction_bytes_limit,
            options.hard_pending_compaction_bytes_limit,
            options.max_compaction_bytes,
        };
        RocksOptionsBackup newOptions = *oldOptions;
        newOptions.disable_auto_compactions = true;
        newOptions.level0_file_num_compaction_trigger = (1 << 30);
        newOptions.level0_slowdown_writes_trigger = (1 << 30);
        newOptions.level0_stop_writes_trigger = (1 << 30);
        newOptions.max_write_buffer_number = 6;
        newOptions.max_background_compactions = 2;
        newOptions.base_background_compactions = 2;
        newOptions.write_buffer_size = rocksGlobalOptions.initialWriteBufferSize;
        const size_t align = 4 * 1024;
        newOptions.arena_block_size = (((newOptions.write_buffer_size + 7) / 8 + align - 1) & ~(align - 1));
        newOptions.soft_pending_compaction_bytes_limit = 0;
        newOptions.hard_pending_compaction_bytes_limit = 0;
        newOptions.max_compaction_bytes = (1ull << 60);
        rocksdb::Status s;
        s = _db->SetDBOptions(newOptions.dumpDBOptions());
        dassert(s.ok());
        s = _db->SetOptions(newOptions.dumpCFOptions());
        dassert(s.ok());
        (void)s;
        return oldOptions;
    }

    void RocksEngine::finishInitialSync(void *prepareResult) {
        if (!rocksGlobalOptions.terarkEnable) {
            dassert(!prepareResult);
            return;
        }
        auto oldOptions = (RocksOptionsBackup*)prepareResult;
        // these calls may very slow ...
        rocksdb::Status s;
        s = _db->CompactRange(nullptr, nullptr, true, oldOptions->num_levels - 1);
        dassert(s.ok());
        s = _db->SetDBOptions(oldOptions->dumpDBOptions());
        dassert(s.ok());
        s = _db->SetOptions(oldOptions->dumpCFOptions());
        dassert(s.ok());
        (void)s;
        delete oldOptions;
    }

    void RocksEngine::setJournalListener(JournalListener* jl) {
        _durabilityManager->setJournalListener(jl);
    }

    int64_t RocksEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);

        auto indexIter = _identIndexMap.find(ident);
        if (indexIter != _identIndexMap.end()) {
            return static_cast<int64_t>(indexIter->second->getSpaceUsedBytes(opCtx));
        }
        auto collectionIter = _identCollectionMap.find(ident);
        if (collectionIter != _identCollectionMap.end()) {
            return collectionIter->second->storageSize(opCtx);
        }

        // this can only happen if collection or index exists, but it's not opened (i.e.
        // getRecordStore or getSortedDataInterface are not called)
        return 1;
    }

    int RocksEngine::flushAllFiles(bool sync) {
        LOG(1) << "RocksEngine::flushAllFiles";
        _counterManager->sync();
        _durabilityManager->waitUntilDurable(true);
        return 1;
    }

    Status RocksEngine::beginBackup(OperationContext* txn) {
        return rocksToMongoStatus(_db->PauseBackgroundWork());
    }

    void RocksEngine::endBackup(OperationContext* txn) { _db->ContinueBackgroundWork(); }

    void RocksEngine::setMaxWriteMBPerSec(int maxWriteMBPerSec) {
        _maxWriteMBPerSec = maxWriteMBPerSec;
        _rateLimiter->SetBytesPerSecond(static_cast<int64_t>(_maxWriteMBPerSec) << 20);
    }

    Status RocksEngine::backup(const std::string& path) {
        rocksdb::Checkpoint* checkpoint;
        auto s = rocksdb::Checkpoint::Create(_db.get(), &checkpoint);
        if (s.ok()) {
            s = checkpoint->CreateCheckpoint(path);
        }
        delete checkpoint;
        return rocksToMongoStatus(s);
    }

    std::unordered_set<uint32_t> RocksEngine::getDroppedPrefixes() const {
        stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
        // this will copy the set. that way compaction filter has its own copy and doesn't need to
        // worry about thread safety
        return _droppedPrefixes;
    }

    // non public api
    Status RocksEngine::_createIdent(StringData ident, BSONObjBuilder* configBuilder, bool isOplog) {
        BSONObj config;
        uint32_t prefix = 0;
        {
            stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
            if (_identMap.find(ident) != _identMap.end()) {
                // already exists
                return Status::OK();
            }

            prefix = ++_maxPrefix;
            configBuilder->append("prefix", static_cast<int32_t>(prefix));
            configBuilder->append("is_oplog", isOplog);

            config = std::move(configBuilder->obj());
            _identMap[ident] = config.copy();
        }

        BSONObjBuilder builder;

        auto s = _db->Put(rocksdb::WriteOptions(), _cfHandles[_metaCFIndex], kMetadataPrefix + ident.toString(),
                          rocksdb::Slice(config.objdata(), config.objsize()));

        // oplog always put <prefix> key , terark engine don't like this ...
        if (s.ok() && (isOplog || !rocksGlobalOptions.terarkEnable)) {
            // As an optimization, add a key <prefix> to the DB
            std::string encodedPrefix(encodePrefix(prefix));
            s = _db->Put(rocksdb::WriteOptions(), _cfHandles[isOplog ? _oplogCFIndex : _metaCFIndex],
                         encodedPrefix, rocksdb::Slice());
        }

        return rocksToMongoStatus(s);
    }

    BSONObj RocksEngine::_getIdentConfig(StringData ident) {
        stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
        auto identIter = _identMap.find(ident);
        invariant(identIter != _identMap.end());
        return identIter->second.copy();
    }

    std::string RocksEngine::_extractPrefix(const BSONObj& config) {
        return encodePrefix(config.getField("prefix").numberInt());
    }

    rocksdb::Options RocksEngine::_options() const {
        // default options
        rocksdb::Options options;
        options.rate_limiter = _rateLimiter;
        rocksdb::BlockBasedTableOptions table_options;
        table_options.block_cache = _block_cache;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        table_options.block_size = 16 * 1024; // 16KB
        table_options.format_version = 2;
        if (rocksGlobalOptions.terarkEnable) {
            rocksdb::TerarkZipTableOptions terark_zip_table_options;
            terark_zip_table_options.indexNestLevel = rocksGlobalOptions.indexNestLevel;
            terark_zip_table_options.indexTempLevel = rocksGlobalOptions.indexTempLevel;
            terark_zip_table_options.checksumLevel = rocksGlobalOptions.checksumLevel;
            if (rocksGlobalOptions.entropyAlgo == "none") {
                terark_zip_table_options.entropyAlgo = rocksdb::TerarkZipTableOptions::kNoEntropy;
            }
            else if (rocksGlobalOptions.entropyAlgo == "huffman") {
                terark_zip_table_options.entropyAlgo = rocksdb::TerarkZipTableOptions::kHuffman;
            }
            else if (rocksGlobalOptions.entropyAlgo == "FSE") {
                terark_zip_table_options.entropyAlgo = rocksdb::TerarkZipTableOptions::kFSE;
            }
            else {
                log() << "Unknown entropyAlgo, will use default (none)";
                terark_zip_table_options.entropyAlgo = rocksdb::TerarkZipTableOptions::kNoEntropy;
            }
            terark_zip_table_options.terarkZipMinLevel = rocksGlobalOptions.terarkZipMinLevel;
            terark_zip_table_options.useSuffixArrayLocalMatch = rocksGlobalOptions.useSuffixArrayLocalMatch;
            terark_zip_table_options.warmUpIndexOnOpen = rocksGlobalOptions.warmUpIndexOnOpen;
            terark_zip_table_options.warmUpValueOnOpen = rocksGlobalOptions.warmUpValueOnOpen;
            terark_zip_table_options.estimateCompressionRatio = float(rocksGlobalOptions.estimateCompressionRatio);
            terark_zip_table_options.sampleRatio = rocksGlobalOptions.sampleRatio;
            terark_zip_table_options.keyPrefixLen = 4;
            terark_zip_table_options.offsetArrayBlockUnits = 128;
            terark_zip_table_options.localTempDir = rocksGlobalOptions.localTempDir;
            terark_zip_table_options.indexType = rocksGlobalOptions.indexType;
            terark_zip_table_options.softZipWorkingMemLimit = uint64_t(rocksGlobalOptions.softZipWorkingMemLimit);
            terark_zip_table_options.hardZipWorkingMemLimit = uint64_t(rocksGlobalOptions.hardZipWorkingMemLimit);
            terark_zip_table_options.smallTaskMemory = uint64_t(rocksGlobalOptions.smallTaskMemory);
            terark_zip_table_options.indexCacheRatio = rocksGlobalOptions.indexCacheRatio;
            terark_zip_table_options.minPreadLen = rocksGlobalOptions.minPreadLen;
            terark_zip_table_options.cacheShards = rocksGlobalOptions.cacheShards;
            terark_zip_table_options.cacheCapacityBytes = rocksGlobalOptions.cacheCapacityBytes;
            options.table_factory.reset(rocksdb::NewTerarkZipTableFactory(terark_zip_table_options,
                                                                          rocksdb::NewBlockBasedTableFactory(table_options)));

            options.allow_mmap_reads = true;
            options.compaction_style = rocksdb::kCompactionStyleUniversal;
            options.compaction_options_universal.min_merge_width = 5;
            options.compaction_options_universal.max_merge_width = 20;
            options.num_levels = rocksGlobalOptions.numLevels;
            options.level0_slowdown_writes_trigger = 12;
            options.level0_file_num_compaction_trigger = 6;
            options.max_write_buffer_number = 4;
            options.base_background_compactions = 4;
            options.max_background_compactions = 8;
            options.max_background_flushes = 3;
            options.soft_rate_limit = 2.5;
            options.hard_rate_limit = 3;
            options.max_subcompactions = 4;
            options.level_compaction_dynamic_level_bytes = true;

            unsigned long long targetFileSizeBase = rocksGlobalOptions.targetFileSizeBase;
            int targetFileSizeMultiplier = rocksGlobalOptions.targetFileSizeMultiplier;
            if (targetFileSizeBase == 0) {
                targetFileSizeBase = 512ULL << 20;
            }
            if (targetFileSizeMultiplier == 0) {
                targetFileSizeMultiplier = 2;
            }
            options.write_buffer_size = targetFileSizeBase;
            options.target_file_size_base = targetFileSizeBase;
            options.target_file_size_multiplier = targetFileSizeMultiplier;
            options.max_bytes_for_level_base = targetFileSizeBase * 4;
            options.max_bytes_for_level_multiplier = targetFileSizeMultiplier;

            options.env->SetBackgroundThreads(options.max_background_compactions, rocksdb::Env::LOW);
            options.env->SetBackgroundThreads(options.max_background_flushes, rocksdb::Env::HIGH);

            terark::DictZipBlobStore_setZipThreads(rocksGlobalOptions.terarkZipThreads);
        }
        else {
            options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

            options.level0_slowdown_writes_trigger = 8;
            options.max_write_buffer_number = 4;
            options.max_background_compactions = 8;
            options.max_background_flushes = 2;
            options.soft_rate_limit = 2.5;
            options.hard_rate_limit = 3;
            options.level_compaction_dynamic_level_bytes = true;

            unsigned long long targetFileSizeBase = rocksGlobalOptions.targetFileSizeBase;
            int targetFileSizeMultiplier = rocksGlobalOptions.targetFileSizeMultiplier;
            if (targetFileSizeBase == 0) {
                targetFileSizeBase = 64ULL << 20;
            }
            if (targetFileSizeMultiplier == 0) {
                targetFileSizeMultiplier = 1;
            }
            options.write_buffer_size = targetFileSizeBase;
            options.target_file_size_base = targetFileSizeBase;
            options.target_file_size_multiplier = targetFileSizeMultiplier;
            options.max_bytes_for_level_base = targetFileSizeBase * 8;
            options.max_bytes_for_level_multiplier = targetFileSizeMultiplier;
        }
        // This means there is no limit on open files. Make sure to always set ulimit so that it can
        // keep all RocksDB files opened.
        options.max_open_files = -1;
        options.optimize_filters_for_hits = true;
        options.compaction_filter_factory.reset(new PrefixDeletingCompactionFilterFactory(this));
        options.enable_thread_tracking = true;
        // Enable concurrent memtable
        options.allow_concurrent_memtable_write = true;
        options.enable_write_thread_adaptive_yield = true;

        options.compression_per_level.resize(3);
        options.compression_per_level[0] = rocksdb::kNoCompression;
        options.compression_per_level[1] = rocksdb::kNoCompression;
        if (rocksGlobalOptions.compression == "snappy") {
            options.compression_per_level[2] = rocksdb::kSnappyCompression;
        } else if (rocksGlobalOptions.compression == "zlib") {
            options.compression_per_level[2] = rocksdb::kZlibCompression;
        } else if (rocksGlobalOptions.compression == "none") {
            options.compression_per_level[2] = rocksdb::kNoCompression;
        } else if (rocksGlobalOptions.compression == "lz4") {
            options.compression_per_level[2] = rocksdb::kLZ4Compression;
        } else if (rocksGlobalOptions.compression == "lz4hc") {
            options.compression_per_level[2] = rocksdb::kLZ4HCCompression;
        } else {
            log() << "Unknown compression, will use default (snappy)";
            options.compression_per_level[2] = rocksdb::kSnappyCompression;
        }

        options.statistics = _statistics;

        // create the DB if it's not already present
        options.create_if_missing = true;
        options.wal_dir = _path + "/journal";

        // allow override
        if (!rocksGlobalOptions.configString.empty()) {
            rocksdb::Options base_options(options);
            auto s = rocksdb::GetOptionsFromString(base_options, rocksGlobalOptions.configString,
                                                   &options);
            if (!s.ok()) {
                log() << "Invalid rocksdbConfigString \"" << rocksGlobalOptions.configString
                      << "\"";
                invariantRocksOK(s);
            }
        }

        return options;
    }

    void RocksEngine::ReclaimOplog() {
        Client::initThread("RocksEngine::ReclaimOplog");
        auto txn = cc().makeOperationContext();
        std::unique_lock<std::mutex> oplogReclaimLock(_oplogReclaimOMutex);
        std::unordered_map<uint32_t, std::shared_ptr<RocksRecordStore>> store;
        std::string key = kOplogCollectorPrefix;
        const size_t len = key.size();
        key.resize(len + 4);
        size_t oplogCappedSize = 0;
        bool retry = false;
        while (true) {
            store.clear();
            _oplogReclaimOCV.wait_for(oplogReclaimLock, std::chrono::milliseconds(retry ? 100 : 10000));
            retry = false;
            if (_shuttingDown) {
                break;
            }
            if(!_db) {
                continue;
            }
            if (!oplogMutex.try_lock()) {
                retry = true;
                continue;
            }
            std::unique_lock<std::mutex> oplogLock(oplogMutex, std::adopt_lock);
            size_t newCappedSize = 0;

            bool needReclaim = false;
            {
                stdx::lock_guard <stdx::mutex> lk(_oplogMapMutex);
                for(auto &pair : _oplogPrefixMap) {
                    auto ib = store.emplace(pair.first, pair.second.lock());
                    // make sure the record store alive ...
                    if(!ib.first->second) {
                        retry = true;
                        break;
                    }
                    auto &s = ib.first->second;
                    newCappedSize += s->cappedMaxSize();
                    needReclaim = needReclaim || s->dataSize(txn.get()) > s->cappedMaxSize();
                }
            }
            if (retry || store.empty()) {
                continue;
            }
            if (newCappedSize > 0 && oplogCappedSize != newCappedSize) {
                // capped size changed , update memtable size
                oplogCappedSize = newCappedSize;
                StringBuilder sb;
                size_t writeBufferSize = rocksGetWriteBufferSize(oplogCappedSize);
                std::unordered_map<std::string, std::string> options;
                sb.reset(16);
                sb << writeBufferSize;
                options.emplace("write_buffer_size", sb.str());
                sb.reset(16);
                const size_t align = 4 * 1024;
                sb << (((writeBufferSize + 7) / 8 + align - 1) & ~(align - 1));
                options.emplace("arena_block_size", sb.str());
                auto s = _db->SetOptions(_cfHandles[_oplogCFIndex], options);
                dassert(s.ok());
                rocksdb::FlushOptions fo;
                fo.wait = false;
                s = _db->Flush(fo, _cfHandles[_oplogCFIndex]);
                dassert(s.ok());
                retry = true;
            }
            if (!needReclaim) {
                continue;
            }
            rocksdb::ColumnFamilyMetaData meta;
            rocksdb::TablePropertiesCollection prop;
            _db->GetColumnFamilyMetaData(_cfHandles[_oplogCFIndex], &meta);
            _db->GetPropertiesOfAllTables(_cfHandles[_oplogCFIndex], &prop);
            auto &l0 = meta.levels[0];
            bool fail = false;
            if (l0.files.size() > 1) {
                typedef std::pair<const rocksdb::SstFileMetaData*, const rocksdb::TableProperties*> info_t;
                std::vector<info_t> files;
                auto comp = [&](const info_t& l, const info_t& r) {
                    return rocksGetSstIndex(l.first->name) > rocksGetSstIndex(r.first->name);
                };
                size_t value_size = 0;
                // bind meta & prop , build min heap
                for (auto &f : l0.files) {
                    auto find = prop.find(f.db_path + f.name);
                    if (find == prop.end()) {
                        retry = true;
                        break;
                    }
                    auto prop = find->second.get();
                    value_size += prop->raw_value_size;
                    files.emplace_back(&f, prop);
                    std::push_heap(files.begin(), files.end(), comp);
                }
                if (retry) {
                    continue;
                }
                do
                {
                    auto &f = *files.front().first;     // MetaData ref
                    auto prop = files.front().second;   // Properties ptr
                    // check any ns last oplog time greater than oplog min optime
                    auto &user_prop = prop->user_collected_properties;
                    MongoRocksOplogPropertiesCollector::Info sstInfo;
                    for(auto &pair : store) {
                        uint32_t prefix = endian::nativeToBig(pair.first);
                        auto &s = pair.second;
                        memcpy(&key.front() + len, &prefix, 4);     // for MongoRocksOplogPropertiesCollector
                        auto find = user_prop.find(key);
                        if(find != user_prop.end()) {
                            MongoRocksOplogPropertiesCollector::Decode(*find, &sstInfo);
                            if(s->dataSize(txn.get()) < s->cappedMaxSize() + sstInfo.dataSize) {
                                fail = false;
                                break;
                            }
                        }
                    }
                    if (fail) {
                        break;
                    }
                    // update oplog collection numRecords & dataSize before delete sst
                    auto& opPrefix = kOplogCollectorPrefix;
                    for (auto it = user_prop.lower_bound(opPrefix);
                         it != user_prop.end() && it->first.compare(0, opPrefix.size(), opPrefix) == 0;
                         ++it) {
                        uint32_t prefix = MongoRocksOplogPropertiesCollector::Decode(*it, &sstInfo);
                        auto find = store.find(prefix);
                        if(find != store.end()) {
                            find->second->updateStats(txn.get(),
                                                      -int64_t(sstInfo.numRecords),
                                                      -int64_t(sstInfo.dataSize));
                        }
                    }
                    _db->DeleteFile(f.name);
                    value_size -= prop->raw_value_size;
                    std::pop_heap(files.begin(), files.end(), comp);
                    files.pop_back();
                    retry = true;
                } while (files.size() > 1);
            }
        }
    }
}
