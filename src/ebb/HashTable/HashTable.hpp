/*
  EbbRT: Distributed, Elastic, Runtime
  Copyright (C) 2013 SESA Group, Boston University

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as
  published by the Free Software Foundation, either version 3 of the
  License, or (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#ifndef EBBRT_EBB_HASHTABLE_HASHTABLE_HPP
#define EBBRT_EBB_HASHTABLE_HASHTABLE_HPP

#include <functional>

#include "ebb/ebb.hpp"

namespace ebbrt {
  class HashTable : public EbbRep {
  public:
    virtual void Get(const char* key,
                     size_t key_size,
                     std::function<void(const char*, size_t)> func,
                     std::function<void()> sent = nullptr) = 0;
    virtual void Set(const char* key,
                     size_t key_size,
                     const char* val,
                     size_t val_size,
                     std::function<void()> sent = nullptr) = 0;
    virtual void SyncGet(const char* key,
                         size_t key_size,
                         uint32_t waitfor,
                         std::function<void(const char*, size_t)> func,
                         std::function<void()> sent = nullptr) = 0;
    virtual void SyncSet(const char* key,
                         size_t key_size,
                         const char* val,
                         size_t val_size,
                         uint32_t delta,
                         std::function<void()> sent = nullptr) = 0;
    virtual void Increment(const char* key,
                           size_t key_size,
                           std::function<void(uint32_t)> func,
                           std::function<void()> sent = nullptr) = 0;
  };
  const EbbRef<HashTable> hashtable =
    EbbRef<HashTable>(lrt::trans::find_static_ebb_id("HashTable"));
}
#endif
