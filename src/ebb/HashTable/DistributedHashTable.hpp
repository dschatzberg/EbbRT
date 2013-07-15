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
#ifndef EBBRT_EBB_HASHTABLE_DISTRIBUTEDHASHTABLE_HPP
#define EBBRT_EBB_HASHTABLE_DISTRIBUTEDHASHTABLE_HPP

#include <functional>
#include <unordered_map>
#include "ebb/ebb.hpp"
#include "ebb/HashTable/HashTable.hpp"

namespace ebbrt {
  class DistributedHashTable : public HashTable {
  public:
    static EbbRoot* ConstructRoot();

    DistributedHashTable();

    virtual void Get(const char* key,
                     size_t key_size,
                     std::function<void(const char*, size_t)> func,
                     std::function<void()> sent = nullptr) override;
    // virtual void Set(std::string key, std::string val) override;
    virtual void HandleMessage(NetworkId from, const char* buf,
                               size_t len) override;
  private:
    enum DHTOp {
      //      SET,
      GET_REQUEST,
      GET_RESPONSE
    } type;

    struct GetRequest {
      DHTOp op;
      unsigned op_id;
    };

    struct GetResponse {
      DHTOp op;
      unsigned op_id;
    };

    void HandleGetRequest(NetworkId from, const GetRequest& req,
                          const char* key, size_t len);
    void HandleGetResponse(const GetResponse& resp,
                           const char* val, size_t len);
    inline NetworkId home(size_t h )
    {
      NetworkId id;
      //FIXME: MPI specific
      id.rank = h % nodecount_;
      return id;
    }

    inline bool local(NetworkId i )
    {
      //FIXME: MPI specific
      return i.rank == myid_.rank;
    }

    NetworkId myid_;
    unsigned int nodecount_;
    std::unordered_map<std::string, std::string> table_;
    std::atomic_uint op_id_;
    Spinlock lock_;
    std::unordered_map<unsigned,
                       std::function<void(const char*, size_t)> > cb_map_;
  };
}
#endif
