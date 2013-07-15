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
#include <iostream>
#include <limits>

#include <mpi.h>

#include "ebb/SharedRoot.hpp"
#include "ebb/HashTable/DistributedHashTable.hpp"
#include "ebb/MessageManager/MessageManager.hpp"

ebbrt::DistributedHashTable::DistributedHashTable() {
  if (MPI_Comm_rank(MPI_COMM_WORLD, &myid_.rank) != MPI_SUCCESS) {
    throw std::runtime_error("MPI_Comm_rank failed");
  }

  int size;
  if (MPI_Comm_size(MPI_COMM_WORLD, &size) != MPI_SUCCESS) {
    throw std::runtime_error("MPI_Comm_size failed");
  }
  nodecount_ = size;
}

ebbrt::EbbRoot*
ebbrt::DistributedHashTable::ConstructRoot()
{
  return new SharedRoot<DistributedHashTable>;
}

void
ebbrt::DistributedHashTable::Get(const char* key,
                                 size_t key_size,
                                 std::function<void(const char*, size_t)> cb,
                                 std::function<void()> sent)
{
  auto hasher = table_.hash_function();
  auto location = home(hasher(std::string(key, key_size)));
  if (local(location)) {
    lock_.Lock();
    auto it = table_.find(std::string(key, key_size));
    //FIXME: do this asynchronously
    if (it == table_.end()) {
      lock_.Unlock();
      cb(nullptr, 0);
    } else {
      std::string str = it->second;
      lock_.Unlock();
      cb(str.c_str(), str.length());
    }
    if (sent) {
      sent();
    }
  } else {
    auto header = new GetRequest;
    header->op = GET_REQUEST;
    auto op_id = op_id_.fetch_add(1, std::memory_order_relaxed);
    header->op_id = op_id;

    BufferList list = {
      std::pair<const void*, size_t>(header, sizeof(GetRequest)),
      std::pair<const void*, size_t>(key, key_size)
    };

    lock_.Lock();
    cb_map_[op_id] = cb;
    lock_.Unlock();
    std::cout << myid_.rank << ": Sending GET to " << location.rank << std::endl;
    message_manager->Send(location, hashtable, std::move(list),
                          [=]() {
                            delete header;
                            if (sent) {
                              sent();
                            }
                          });
  }
}

void
ebbrt::DistributedHashTable::Set(const char* key,
                                 size_t key_size,
                                 const char* val,
                                 size_t val_size,
                                 std::function<void()> sent)
{
  auto hasher = table_.hash_function();
  auto location = home(hasher(std::string(key, key_size)));
  if (local(location)) {
    lock_.Lock();
    table_[std::string(key, key_size)] = std::string(val, val_size);
    lock_.Unlock();
    //FIXME: do this asynchronously
    if (sent) {
      sent();
    }
  } else {
    auto header = new SetRequest;
    header->op = SET_REQUEST;
    header->key_size = key_size;

    BufferList list = {
      std::pair<const void*, size_t>(header, sizeof(SetRequest)),
      std::pair<const void*, size_t>(key, key_size),
      std::pair<const void*, size_t>(val, val_size)
    };

    std::cerr << "Sending a Set message to " << location.rank << std::endl;

    message_manager->Send(location, hashtable, std::move(list),
                          [=]() {
                            delete header;
                            if (sent) {
                              sent();
                            }
                          });
  }
}

void
ebbrt::DistributedHashTable::SyncGet(const char* key,
                                     size_t key_size,
                                     uint32_t waitfor,
                                     std::function<void(const char*, size_t)> cb,
                                     std::function<void()> sent)
{
  auto hasher = table_.hash_function();
  auto location = home(hasher(std::string(key, key_size)));
  if (local(location)) {
    lock_.Lock();
    auto& sync_ent = sync_table_[std::string(key, key_size)];
    if (sync_ent.first < waitfor) {
      //Append callback
      auto op_id = op_id_.fetch_add(1, std::memory_order_relaxed);
      cb_map_[op_id] = cb;
      sync_ent.second.insert(std::make_pair(waitfor,
                                            std::make_pair(myid_.rank, op_id)));
      lock_.Unlock();
    } else {
      //FIXME: do this asynchronously
      auto it = table_.find(std::string(key, key_size));
      if (it == table_.end()) {
        lock_.Unlock();
        cb(nullptr, 0);
      } else {
        std::string str = it->second;
        lock_.Unlock();
        cb(str.c_str(), str.length());
      }
    }
    //FIXME: do this asynchronously
    if (sent) {
      sent();
    }
  } else {
    auto header = new SyncGetRequest;
    header->op = SYNC_GET_REQUEST;
    auto op_id = op_id_.fetch_add(1, std::memory_order_relaxed);
    header->op_id = op_id;
    header->waitfor = waitfor;

    BufferList list = {
      std::pair<const void*, size_t>(header, sizeof(SyncGetRequest)),
      std::pair<const void*, size_t>(key, key_size)
    };

    lock_.Lock();
    cb_map_[op_id] = cb;
    lock_.Unlock();
    std::cout << myid_.rank << ": Sending SYNC GET to " << location.rank << std::endl;
    message_manager->Send(location, hashtable, std::move(list),
                          [=]() {
                            delete header;
                            if (sent) {
                              sent();
                            }
                          });
  }
}

void
ebbrt::DistributedHashTable::SyncSet(const char* key,
                                     size_t key_size,
                                     const char* val,
                                     size_t val_size,
                                     uint32_t delta,
                                     std::function<void()> sent)
{
  auto hasher = table_.hash_function();
  auto location = home(hasher(std::string(key, key_size)));
  if (local(location)) {
    lock_.Lock();
    table_[std::string(key, key_size)] = std::string(val, val_size);
    auto& sync_ent = sync_table_[std::string(key, key_size)];
    sync_ent.first += delta;
    for (auto it = sync_ent.second.begin();
         it != sync_ent.second.upper_bound(sync_ent.first);
         ++it) {
      if (it->second.first != myid_.rank) {
        auto header = new GetResponse;
        header->op = GET_RESPONSE;
        header->op_id = it->second.second;

        //allocate and copy value
        BufferList list = {
          std::pair<const void*, size_t>(header, sizeof(GetResponse)),
          std::pair<const void*, size_t>(val, val_size)
        };
        NetworkId to;
        to.rank = it->second.first;
        std::cout << myid_.rank << ": Sending GET response to " << to.rank << std::endl;
        message_manager->Send(to, hashtable, std::move(list),
                              [=]() {
                                delete header;
                                if (sent) {
                                  sent();
                                }
                              });
      } else {
        auto cb_it = cb_map_.find(it->second.second);
        assert(cb_it != cb_map_.end());
        auto cb = cb_it->second;
        cb_map_.erase(cb_it);
        //FIXME: do asynchronously
        cb(val, val_size);
      }
    }
    sync_ent.second.erase(sync_ent.second.begin(),
                          sync_ent.second.upper_bound(sync_ent.first));
    lock_.Unlock();
    //FIXME: do this asynchronously
    if (sent) {
      sent();
    }
  } else {
    auto header = new SyncSetRequest;
    header->op = SYNC_SET_REQUEST;
    header->key_size = key_size;
    header->delta = delta;

    BufferList list = {
      std::pair<const void*, size_t>(header, sizeof(SyncSetRequest)),
      std::pair<const void*, size_t>(key, key_size),
      std::pair<const void*, size_t>(val, val_size)
    };

    std::cout << myid_.rank << ": Sending SYNC SET to " << location.rank << std::endl;
    message_manager->Send(location, hashtable, std::move(list),
                          [=]() {
                            delete header;
                            if (sent) {
                              sent();
                            }
                          });
  }
}

void
ebbrt::DistributedHashTable::Increment(const char* key,
                                       size_t key_size,
                                       std::function<void(uint32_t)> func,
                                       std::function<void()> sent)
{
  auto hasher = table_.hash_function();
  auto location = home(hasher(std::string(key, key_size)));
  if (local(location)) {
    lock_.Lock();
    auto val = val_table_[std::string(key, key_size)]++;
    lock_.Unlock();
    //FIXME: do this asynchronously
    func(val);
  } else {
    auto header = new IncrementRequest;
    header->op = INCREMENT_REQUEST;
    auto op_id = op_id_.fetch_add(1, std::memory_order_relaxed);
    header->op_id = op_id;

    BufferList list = {
      std::pair<const void*, size_t>(header, sizeof(IncrementRequest)),
      std::pair<const void*, size_t>(key, key_size)
    };

    lock_.Lock();
    inc_cb_map_[op_id] = func;
    lock_.Unlock();
    std::cout << myid_.rank << ": Sending INCREMENT to " << location.rank << std::endl;
    message_manager->Send(location, hashtable, std::move(list),
                          [=]() {
                            delete header;
                            if (sent) {
                              sent();
                            }
                          });
  }
}

void
ebbrt::DistributedHashTable::HandleMessage(NetworkId from,
                                           const char* buf,
                                           size_t len)
{
  assert(len >= sizeof(DHTOp));

  auto op = reinterpret_cast<const DHTOp*>(buf);
  switch (*op) {
  case DHTOp::GET_REQUEST:
    assert(len >= sizeof(GetRequest));
    HandleGetRequest(from, *reinterpret_cast<const GetRequest*>(buf),
                     buf + sizeof(GetRequest), len - sizeof(GetRequest));
    break;
  case DHTOp::GET_RESPONSE:
    assert(len >= sizeof(GetResponse));
    HandleGetResponse(*reinterpret_cast<const GetResponse*>(buf),
                      buf + sizeof(GetResponse), len - sizeof(GetResponse));
    break;
  case DHTOp::SET_REQUEST:
    assert(len >= sizeof(SetRequest));
    HandleSetRequest(*reinterpret_cast<const SetRequest*>(buf),
                     buf + sizeof(SetRequest), len - sizeof(SetRequest));
    break;
  case DHTOp::SYNC_GET_REQUEST:
    assert(len >= sizeof(SyncGetRequest));
    HandleSyncGetRequest(from, *reinterpret_cast<const SyncGetRequest*>(buf),
                         buf + sizeof(SyncGetRequest),
                         len - sizeof(SyncGetRequest));
    break;
  case DHTOp::SYNC_SET_REQUEST:
    assert(len >= sizeof(SyncSetRequest));
    HandleSyncSetRequest(*reinterpret_cast<const SyncSetRequest*>(buf),
                         buf + sizeof(SyncSetRequest),
                         len - sizeof(SyncSetRequest));
    break;
  case DHTOp::INCREMENT_REQUEST:
    assert(len >= sizeof(IncrementRequest));
    HandleIncrementRequest(from, *reinterpret_cast<const IncrementRequest*>(buf),
                           buf + sizeof(IncrementRequest),
                           len - sizeof(IncrementRequest));
    break;
  case DHTOp::INCREMENT_RESPONSE:
    assert(len >= sizeof(IncrementResponse));
    HandleIncrementResponse(*reinterpret_cast<const IncrementResponse*>(buf));
    break;
  }
}

void
ebbrt::DistributedHashTable::HandleGetRequest(NetworkId from,
                                              const GetRequest& req,
                                              const char* key, size_t len)
{
  std::cout << myid_.rank << ": Got GET request from " << from.rank << std::endl;
  lock_.Lock();
  auto it = table_.find(std::string(key, len));
  auto header = new GetResponse;
  header->op = GET_RESPONSE;
  header->op_id = req.op_id;
  std::cout << myid_.rank << ": Sending GET response to " << from.rank << std::endl;
  if (it == table_.end()) {
    lock_.Unlock();
    BufferList list = {
      std::pair<const void*, size_t>(header, sizeof(GetResponse))
    };
    message_manager->Send(from, hashtable, std::move(list),
                          [=]() {
                            delete header;
                          });
  } else {
    //allocate and copy value
    auto val = new std::string(it->second);
    lock_.Unlock();
    BufferList list = {
      std::pair<const void*, size_t>(header, sizeof(GetResponse)),
      std::pair<const void*, size_t>(val->c_str(), val->length())
    };
    message_manager->Send(from, hashtable, std::move(list),
                          [=]() {
                            delete val;
                            delete header;
                          });
  }
}

void
ebbrt::DistributedHashTable::HandleGetResponse(const GetResponse& resp,
                                               const char* val, size_t len)
{
  std::cout << myid_.rank << ": Got GET Response" << std::endl;
  lock_.Lock();
  auto it = cb_map_.find(resp.op_id);
  assert(it != cb_map_.end());
  auto cb = it->second;
  cb_map_.erase(it);
  lock_.Unlock();
  if (len == 0) {
    cb(nullptr, 0);
  } else {
    cb(val, len);
  }
}

void
ebbrt::DistributedHashTable::HandleSetRequest(const SetRequest& req,
                                              const char* buf, size_t len)
{
  std::cout << myid_.rank << ": Got SET Request" << std::endl;
  assert(local(home(table_.hash_function()(std::string(buf, req.key_size)))));
  lock_.Lock();
  table_[std::string(buf, req.key_size)] = std::string(buf + req.key_size,
                                                       len - req.key_size);
  lock_.Unlock();
}

void
ebbrt::DistributedHashTable::HandleSyncGetRequest(NetworkId from,
                                                  const SyncGetRequest& req,
                                                  const char* key, size_t len)
{
  std::cout << myid_.rank << ": Got Sync GET request from " << from.rank <<
    std::endl;
  assert(local(home(table_.hash_function()(std::string(key, len)))));
  lock_.Lock();
  auto& sync_ent = sync_table_[std::string(key, len)];
  if (sync_ent.first < req.waitfor) {
    sync_ent.second.insert(std::make_pair(req.waitfor,
                                          std::make_pair(from.rank, req.op_id)));
    lock_.Unlock();
  } else {
    auto it = table_.find(std::string(key, len));
    auto header = new GetResponse;
    header->op = GET_RESPONSE;
    header->op_id = req.op_id;
    std::cout << myid_.rank << ": Sending SYNC GET response to " << from.rank << std::endl;
    if (it == table_.end()) {
      lock_.Unlock();
      BufferList list = {
        std::pair<const void*, size_t>(header, sizeof(GetResponse))
      };
      message_manager->Send(from, hashtable, std::move(list),
                            [=]() {
                              delete header;
                            });
    } else {
      //allocate and copy value
      auto val = new std::string(it->second);
      lock_.Unlock();
      BufferList list = {
        std::pair<const void*, size_t>(header, sizeof(GetResponse)),
        std::pair<const void*, size_t>(val->c_str(), val->length())
      };
      message_manager->Send(from, hashtable, std::move(list),
                            [=]() {
                              delete val;
                              delete header;
                            });
    }
  }
}

void
ebbrt::DistributedHashTable::HandleSyncSetRequest(const SyncSetRequest& req,
                                                  const char* buf, size_t len)
{
  std::cout << myid_.rank << ": Got Sync SET Request" << std::endl;
  assert(local(home(table_.hash_function()(std::string(buf, req.key_size)))));

  lock_.Lock();

  auto& ent = table_[std::string(buf, req.key_size)];
  ent = std::string(buf + req.key_size, len - req.key_size);

  auto& sync_ent = sync_table_[std::string(buf, req.key_size)];
  sync_ent.first += req.delta;
  for (auto it = sync_ent.second.begin();
       it != sync_ent.second.upper_bound(sync_ent.first);
       ++it) {
    if (it->second.first != myid_.rank) {
      auto header = new GetResponse;
      header->op = GET_RESPONSE;
      header->op_id = it->second.second;

      //allocate and copy value
      auto val = new std::string(ent);
      BufferList list = {
        std::pair<const void*, size_t>(header, sizeof(GetResponse)),
        std::pair<const void*, size_t>(val->c_str(), val->length())
      };
      NetworkId to;
      to.rank = it->second.first;
      std::cout << myid_.rank << ": Sending SYNC GET response to " << to.rank << std::endl;
      message_manager->Send(to, hashtable, std::move(list),
                            [=]() {
                              delete val;
                              delete header;
                            });
    } else {
      auto cb_it = cb_map_.find(it->second.second);
      assert(cb_it != cb_map_.end());
      auto cb = cb_it->second;
      cb_map_.erase(cb_it);
      //FIXME: do asynchronously
      cb(buf + req.key_size, len - req.key_size);
    }
  }
  sync_ent.second.erase(sync_ent.second.begin(),
                        sync_ent.second.upper_bound(sync_ent.first));
  lock_.Unlock();
}

void
ebbrt::DistributedHashTable::HandleIncrementRequest(NetworkId from,
                                                    const IncrementRequest& req,
                                                    const char* key,
                                                    size_t len)
{
  std::cout << myid_.rank << ": Got INCREMENT request from " << from.rank << std::endl;
  lock_.Lock();
  auto val = val_table_[std::string(key, len)]++;
  lock_.Unlock();

  auto header = new IncrementResponse;
  header->op = INCREMENT_RESPONSE;
  header->op_id = req.op_id;
  header->val = val;

  BufferList list = {
    std::pair<const void*, size_t>(header, sizeof(IncrementResponse))
  };
  std::cout << myid_.rank << ": Sending INCREMENT response to " << from.rank << std::endl;
  message_manager->Send(from, hashtable, std::move(list),
                        [=]() {
                          delete header;
                        });

}

void
ebbrt::DistributedHashTable::HandleIncrementResponse(const IncrementResponse& resp)
{
  std::cout << myid_.rank << ": Got INCREMENT response" << std::endl;
  lock_.Lock();
  auto it = inc_cb_map_.find(resp.op_id);
  assert(it != inc_cb_map_.end());
  auto cb = it->second;
  inc_cb_map_.erase(it);
  lock_.Unlock();
  cb(resp.val);
}
