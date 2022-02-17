#include "velox/common/caching/cachelib/CachelibDataCache.h"
#include <boost/lexical_cast.hpp>
#include <filesystem>
#include <unordered_map>
#include "glog/logging.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/Context.h"

namespace facebook::velox {

namespace {
std::string getOptionalProperty(
    const std::unordered_map<std::string, std::string>& properties,
    const std::string& name,
    const std::string& defaultValue) {
  auto it = properties.find(name);
  if (it == properties.end()) {
    return defaultValue;
  }
  return it->second;
}

std::string getRequiredProperty(
    const std::unordered_map<std::string, std::string>& properties,
    const std::string& name) {
  auto it = properties.find(name);
  if (it == properties.end()) {
    throw std::runtime_error(
        std::string("Missing configuration property ") + name);
  }
  return it->second;
}
} // namespace

CachelibDataCache::CachelibDataCache(
    std::shared_ptr<const Config> properties,
    const std::string& oncall)
    : DataCache() {
  cachelib::LruAllocator::Config lruConfig;

  if (properties->get<bool>("cachelib.enable_hybrid_cache", false)) {
    LOG(INFO)
        << "STARTUP: Hybrid caching mode enabled. Start hybrid mode setup.";
    // For a full list of adjustable flags refer to:
    // https://www.internalfb.com/intern/wiki/Cache_Library_User_Guides/Configure_HybridCache/
    facebook::cachelib::LruAllocator::NvmCacheConfig nvmConfig;

    nvmConfig.navyConfig.setBlockSize(
        properties->get<int64_t>("cachelib.dipper_navy_block_size", 4096));
    nvmConfig.navyConfig.setReaderAndWriterThreads(
        properties->get<int64_t>("cachelib.dipper_navy_reader_threads", 128),
        properties->get<int64_t>("cachelib.dipper_navy_writer_threads", 32));
    nvmConfig.navyConfig.setNavyReqOrderingShards(properties->get<int64_t>(
        "cachelib.dipper_navy_req_order_shards_power", 20));
    auto& bcConfig = nvmConfig.navyConfig.blockCache().setRegionSize(
        properties->get<int64_t>("cachelib.dipper_navy_region_size", 16) *
        1024 * 1024);
    if (not properties->get<bool>("cachelib.dipper_navy_lru", true)) {
      bcConfig.enableFifo();
    }
    const auto cacheFilePath =
        properties->get("cachelib.dipper_navy_file_name").value();
    const std::filesystem::path cacheFsFilePath(cacheFilePath);
    if (cacheFsFilePath.has_parent_path() and
        std::filesystem::exists(cacheFsFilePath.parent_path())) {
      LOG(INFO) << "STARTUP: Enabling hybrid cache in path " << cacheFilePath;
      nvmConfig.navyConfig.setSimpleFile(
          cacheFilePath,
          properties->get<int64_t>("cachelib.dipper_navy_file_size_gb")
                  .value() *
              1024ul * 1024ul * 1024ul,
          properties->get<bool>("cachelib.dipper_navy_truncate_file", false));
      lruConfig.enableNvmCache(nvmConfig);
    } else {
      LOG(WARNING) << "STARTUP: External storage directory for path "
                   << cacheFilePath.data()
                   << " does not exist, not enabling hybrid caching mode.";
    }

    // We only enable persistence when hybrid mode is enabled because we assume
    // no external storage when it is disabled.
    const auto cachePersistenceDir =
        properties->get("cachelib.cachelib_persistence_dir").value();
    const std::filesystem::path cacheFsPersistenceDir(cachePersistenceDir);
    if (std::filesystem::exists(cacheFsPersistenceDir)) {
      LOG(INFO) << "STARTUP: Enabling cache persistence in dir "
                << cachePersistenceDir;
      lruConfig.enableCachePersistence(cachePersistenceDir);
    } else {
      LOG(WARNING) << "STARTUP: External storage directory "
                   << cachePersistenceDir.data()
                   << " does not exist, not enabling cache persistency.";
    }
  }
  lruConfig
      .setCacheSize(
          properties->get<int64_t>("cache.max-cache-size", 0) * 1024ul * 1024ul)
      .setCacheName("CachelibDataCache")
      .setAccessConfig({/*bucket power=*/25, /*lock power=*/10})
      .validate();
  cache_ = std::make_unique<cachelib::LruAllocator>(lruConfig);
  pool_ = cache_->addPool("default", cache_->getCacheMemoryStats().cacheSize);
}

namespace {

// Cachelib's max individual allocation is 4mb, but that includes the size
// of the key and some internal data, so if we stay with 3mb (and the user
// doesn't use crazy large keys) we should be fine.
constexpr uint64_t kChunkSize = 3 << 20;

// Values over 4mb must be stored in chunks using cachelib's chained
// allocations. This object is the parent (aka head) of that chain.
struct ChunkedValue {
  uint64_t size; // Total size of original value.
  int numChunks;
};

int chunksRequired(uint64_t size) {
  return size / kChunkSize + ((size % kChunkSize != 0) ? 1 : 0);
}

} // namespace

bool CachelibDataCache::put(std::string_view key, std::string_view value) {
  if (value.size() <= kChunkSize) {
    auto handle = cache_->allocate(pool_, key, value.size());
    // Rarely allocation may fail.
    if (handle == nullptr) {
      return false;
    }
    memcpy(handle->getMemory(), value.data(), value.size());
    cache_->insertOrReplace(handle);
    return true;
  }
  const int numChunks = chunksRequired(value.size());
  auto parentHandle = cache_->allocate(pool_, key, sizeof(ChunkedValue));
  if (parentHandle == nullptr) {
    return false;
  }
  auto* chunkedValue =
      reinterpret_cast<ChunkedValue*>(parentHandle->getMemory());
  chunkedValue->size = value.size();
  chunkedValue->numChunks = numChunks;
  const char* pos = value.data();
  for (int i = 0; i < numChunks - 1; ++i) {
    auto chunkHandle = cache_->allocateChainedItem(parentHandle, kChunkSize);
    if (chunkHandle == nullptr) {
      return false;
    }
    memcpy(chunkHandle->getMemory(), pos, kChunkSize);
    pos += kChunkSize;
    cache_->addChainedItem(parentHandle, std::move(chunkHandle));
  }
  const uint64_t remainder =
      value.size() - static_cast<uint64_t>(numChunks - 1) * kChunkSize;
  if (remainder > 0) {
    auto remainderHandle = cache_->allocateChainedItem(parentHandle, remainder);
    if (remainderHandle == nullptr) {
      return false;
    }
    memcpy(remainderHandle->getMemory(), pos, remainder);
    cache_->addChainedItem(parentHandle, std::move(remainderHandle));
  }
  cache_->insertOrReplace(parentHandle);
  return true;
}

bool CachelibDataCache::get(std::string_view key, uint64_t size, void* buf) {
  if (size <= kChunkSize) {
    auto handle = cache_->find(key);
    if (handle == nullptr || handle->getSize() != size) {
      return false;
    }
    memcpy(buf, handle->getMemory(), size);
    return true;
  }
  auto parentHandle = cache_->find(key);
  if (parentHandle == nullptr) {
    return false;
  }
  auto* chunkedValue =
      reinterpret_cast<const ChunkedValue*>(parentHandle->getMemory());
  if (chunkedValue->size != size) {
    LOG(ERROR) << "Cached value size did not match requested size.";
    return false;
  }
  auto chainedAllocs = cache_->viewAsChainedAllocs(parentHandle);
  DCHECK_EQ(chunkedValue->numChunks, chainedAllocs.computeChainLength());
  char* pos = static_cast<char*>(buf);
  pos += size;
  // Chained allocations are LIFO, meaning the last put chained item will be the
  // first to be read.
  for (const auto& c : chainedAllocs.getChain()) {
    pos -= c.getSize();
    memcpy(pos, c.getMemory(), c.getSize());
  }
  VELOX_CHECK_EQ(pos, static_cast<char*>(buf));
  return true;
}

bool CachelibDataCache::get(std::string_view key, std::string* value) {
  auto handle = cache_->find(key);
  if (handle == nullptr) {
    return false;
  }
  // If the cache item is not chained it will throw a std::invalid_argument when
  // we view it as chained.
  try {
    auto chainedAllocs = cache_->viewAsChainedAllocs(handle);
    auto* chunkedValue = reinterpret_cast<const ChunkedValue*>(handle->getMemory());
    value->resize(chunkedValue->size);
    char* pos = value->data() + value->size();
    // Chained allocations are LIFO, meaning the last put chained item will be
    // the first to be read.
    for (const auto& c : chainedAllocs.getChain()) {
      pos -= c.getSize();
      memcpy(pos, c.getMemory(), c.getSize());
    }
    VELOX_CHECK_EQ(pos, value->data());
    return true;
  } catch (const std::invalid_argument& e) {
    value->resize(handle->getSize());
    memcpy(value->data(), handle->getMemory(), handle->getSize());
  }
  return true;
}
} // namespace facebook::velox
