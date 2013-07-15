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
  auto location = home(hasher(key));
  if (local(location)) {
    lock_.Lock();
    auto it = table_.find(std::string(key));
    //FIXME: do this asynchronously
    if (it == table_.end()) {
      lock_.Unlock();
      cb(nullptr, 0);
    } else {
      std::string str = it->second;
      lock_.Unlock();
      cb(str.c_str(), str.length());
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
    message_manager->Send(location, hashtable, std::move(list),
                          [=]() {
                            delete header;
                            if (sent) {
                              sent();
                            }
                          });
  }
}

// void
// ebbrt::DistributedHashTable::Set(std::string key, std::string val,
//                                  std::function<void()> cb)
// {
//   auto hasher = table_.hash_function();
//   auto location = home(hasher(key));
//   if (local(location)) {
//     table_[key] = val;
//     //FIXME: do this asyncrhonously
//     cb();
//   } else {
//     //message_manager->Send
//   }
// }

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
  }
}

void
ebbrt::DistributedHashTable::HandleGetRequest(NetworkId from,
                                              const GetRequest& req,
                                              const char* key, size_t len)
{
  std::cout << myid_.rank << ": Got request from " << from.rank << std::endl;
  lock_.Lock();
  auto it = table_.find(std::string(key, len));
  auto header = new GetResponse;
  header->op = GET_RESPONSE;
  header->op_id = req.op_id;
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
  std::cout << myid_.rank << ": Got Response" << std::endl;
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
