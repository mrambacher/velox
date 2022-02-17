// Lightweight wrapper over the cachelib library. This library allows
// us to combine an in-memory cache with (optionally) a SSD cache.
// Currently cachelib is fb-only, but an OSS version is in the works,
// ETA Q1, so we may be able to move this code into f4d proper then.

#pragma once

#include <cstdint>
#include <exception>
#include <memory>
#include <string_view>

#include "velox/common/caching/DataCache.h"
#include "cachelib/allocator/CacheAllocator.h"

namespace facebook::velox {

class Config;

class CachelibDataCache final : public DataCache {
 public:
  // Creates an in-memory-only cache of the specified passed in properties with
  // some default options. For more control, use the constructor below.
  explicit CachelibDataCache(
      std::shared_ptr<const Config> properties,
      const std::string& oncall);

  ~CachelibDataCache() final {}

  bool put(std::string_view key, std::string_view value) final;
  bool get(std::string_view key, uint64_t size, void* buf) final;
  bool get(std::string_view key, std::string* value) final;

  int64_t currentSize() const final {
    throw std::logic_error("TODO: implement CachelibDataCache::currentSize");
  }

  int64_t maxSize() const final {
    throw std::logic_error("TODO: implement CachelibDataCache::maxSize");
  }

 private:
  std::unique_ptr<cachelib::LruAllocator> cache_;

  cachelib::PoolId pool_;
};

} // namespace facebook::velox
