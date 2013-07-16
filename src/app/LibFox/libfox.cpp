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

#include <cassert>
#include <cstdlib>
#include <cstring>

#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

extern "C" {
#include "src/app/LibFox/libfox.h"
}

#include "mpi.h"

#include "src/ebbrt.hpp"
#include "src/ebb/HashTable/HashTable.hpp"
#include "src/ebb/MessageManager/MessageManager.hpp"


struct fox_st {
  fox_st(int numproc, int myprocid) : table{ebbrt::hashtable},
    invoke_context{instance}, nprocs{numproc}, procid{myprocid}
  {
    std::mutex m;
    std::condition_variable cv;
    bool finished = false;
    listener = std::thread([&]() {
        ebbrt::Context context{instance};
        context.Activate();
        if (MPI_Barrier(MPI_COMM_WORLD) != MPI_SUCCESS) {
          throw std::runtime_error("MPI_Barrier failed");
        }
        ebbrt::message_manager->StartListening();
        if (MPI_Barrier(MPI_COMM_WORLD) != MPI_SUCCESS) {
          throw std::runtime_error("MPI_Barrier failed");
        }
        {
          std::lock_guard<std::mutex> lk{m};
          finished = true;
          cv.notify_one();
        }
        context.Loop(-1);
      });

    listener.detach();

    {
      std::unique_lock<std::mutex> lk{m};
      cv.wait(lk, [&]{return finished;});
    }
    invoke_context.Activate();
  }

  ebbrt::EbbRef<ebbrt::HashTable> table;
  ebbrt::EbbRT instance;
  ebbrt::Context invoke_context;
  std::thread listener;
  int nprocs;
  int procid;
};

extern "C"
int
fox_new(fox_ptr* fhand_ptr, int nprocs, int procid)
{
  *fhand_ptr = new fox_st(nprocs, procid);
  return 0;
}

extern "C"
int
fox_free(fox_ptr fhand)
{
  while (1)
    ;
  delete fhand;
  return 0;
}

extern "C"
int
fox_flush(fox_ptr fhand, int term)
{
  return 0;
}

extern "C"
int
fox_server_add(fox_ptr fhand, const char *hostlist)
{
  return 0;
}

extern "C"
int
fox_set(fox_ptr fhand,
        const char *key, size_t key_sz,
        const char *value, size_t value_sz)
{
  fhand->table->Set(key, key_sz,
                    value, value_sz);
  return 0;
}

extern "C"
int
fox_get(fox_ptr fhand,
        const char *key, size_t key_sz,
        char **pvalue, size_t *pvalue_sz)
{
  std::mutex m;
  std::condition_variable cv;
  bool finished = false;
  fhand->table->Get(key, key_sz,
                    [&](const char* val, size_t size) {
                      char* buf = static_cast<char*>(malloc(size));
                      std::memcpy(buf, val, size);
                      *pvalue = buf;
                      *pvalue_sz = size;
                      std::lock_guard<std::mutex> lk{m};
                      finished = true;
                      cv.notify_one();
                    });
  std::unique_lock<std::mutex> lk{m};
  cv.wait(lk, [&]{return finished;});
  return 0;
}

extern "C"
int
fox_delete(fox_ptr fhand, const char* key, size_t key_sz)
{
  assert(0);
  return 0;
}

extern "C"
int
fox_sync_set(fox_ptr fhand, unsigned delta,
             const char* key, size_t key_sz,
             const char* value, size_t value_sz)
{
  fhand->table->SyncSet(key, key_sz,
                        value, value_sz,
                        delta);
  return 0;
}

extern "C"
int
fox_sync_get(fox_ptr fhand, unsigned delta,
             const char *key, size_t key_sz,
             char **pvalue, size_t *pvalue_sz)
{
  std::mutex m;
  std::condition_variable cv;
  bool finished = false;
  fhand->table->SyncGet(key, key_sz,
                        delta, [&](const char* val, size_t size) {
                          char* buf = static_cast<char*>(malloc(size));
                          std::memcpy(buf, val, size);
                          *pvalue = buf;
                          *pvalue_sz = size;
                          std::lock_guard<std::mutex> lk{m};
                          finished = true;
                          cv.notify_one();
                        });
  std::unique_lock<std::mutex> lk{m};
  cv.wait(lk, [&]{return finished;});
  return 0;
}

extern "C"
int
fox_broad_set(fox_ptr fhand,
              const char *key, size_t key_sz,
              const char *value, size_t value_sz)
{
  return fox_sync_set(fhand, 1, key, key_sz, value, value_sz);
}

extern "C"
int
fox_broad_get(fox_ptr fhand,
              const char *key, size_t key_sz,
              char **pvalue, size_t *pvalue_sz)
{
  return fox_sync_get(fhand, 1, key, key_sz, pvalue, pvalue_sz);
}

extern "C"
int
fox_queue_set(fox_ptr fhand,
              const char *key, size_t key_sz,
              const char *value, size_t value_sz)
{
  std::string str{key, key_sz};
  str += "_B";
  std::mutex m;
  std::condition_variable cv;
  bool finished = false;
  uint32_t qidx;
  fhand->table->Increment(str.c_str(), str.length(),
                          [&](uint32_t val) {
                            qidx = val;
                            std::lock_guard<std::mutex> lk{m};
                            finished = true;
                            cv.notify_one();
                        });
  {
    std::unique_lock<std::mutex> lk{m};
    cv.wait(lk, [&]{return finished;});
  }

  std::string str2{key, key_sz};
  str2 += std::to_string(qidx);
  fox_sync_set(fhand, 1, str2.c_str(), str2.length(), value, value_sz);

  return 0;
}

extern "C"
int
fox_queue_get(fox_ptr fhand,
              const char *key, size_t key_sz,
              char **pvalue, size_t *pvalue_sz)
{
  std::string str{key, key_sz};
  str += "_F";
  std::mutex m;
  std::condition_variable cv;
  bool finished = false;
  uint32_t qidx;
  fhand->table->Increment(str.c_str(), str.length(),
                          [&](uint32_t val) {
                            qidx = val;
                            std::lock_guard<std::mutex> lk{m};
                            finished = true;
                            cv.notify_one();
                          });
  {
    std::unique_lock<std::mutex> lk{m};
    cv.wait(lk, [&]{return finished;});
  }

  std::string str2{key, key_sz};
  str2 += std::to_string(qidx);
  fox_sync_get(fhand, 1, str2.c_str(), str2.length(), pvalue, pvalue_sz);

  return 0;
}

extern "C"
int
fox_broad_queue_set(fox_ptr fhand,
                    const char *key, size_t key_sz,
                    const char *value, size_t value_sz)
{
  std::string str {key, key_sz};

  for (int idx = 0; idx < fhand->nprocs; ++idx) {
    auto newkey = str;
    newkey += std::to_string(idx);
    fox_queue_set(fhand, newkey.c_str(), newkey.length(),
                  value, value_sz);
  }
  return 0;
}

extern "C"
int
fox_dist_queue_set(fox_ptr fhand,
                   const char *key, size_t key_sz,
                   const char *value, size_t value_sz)
{
  static int oproc = 0;

  std::string str{key, key_sz};
  str += std::to_string((fhand->procid + oproc) % fhand->nprocs);
  fox_queue_set(fhand, str.c_str(), str.length(), value, value_sz);

  oproc = (oproc + 1) % fhand->nprocs;
  return 0;
}

extern "C"
int
fox_dist_queue_get(fox_ptr fhand,
                   const char *key, size_t key_sz,
                   char **pvalue, size_t *pvalue_sz)
{
  std::string str{key, key_sz};

  str += std::to_string(fhand->procid);

  return fox_queue_get(fhand, str.c_str(), str.length(), pvalue, pvalue_sz);
}

extern "C"
int
fox_reduce_set(fox_ptr fhand,
               const char *key, size_t key_sz,
               const char *value, size_t value_sz)
{
  std::string str{key, key_sz};

  str += std::to_string(fhand->procid);

  return fox_sync_set(fhand, 1, str.c_str(), str.length(), value, value_sz);
}

extern "C"
int
fox_reduce_get(fox_ptr fhand,
               const char *key, size_t key_sz,
               char *pvalue, size_t pvalue_sz,
               void (*reduce)(void *out, void *in))
{
  for (int idx = 0; idx < fhand->nprocs; ++idx) {
    std::string str{key, key_sz};
    str += std::to_string(idx);

    char* data_ptr;
    size_t data_sz;
    fox_sync_get(fhand, 1, str.c_str(), str.length(), &data_ptr, &data_sz);

    reduce(pvalue, data_ptr);
    free(data_ptr);
  }

  return 0;
}

extern "C"
int
fox_collect(fox_ptr fhand,
            const char *key, size_t key_sz,
            int root, int opt)
{
  assert(0);
  return 0;
}
