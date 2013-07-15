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
#include <mpi.h>

#include "app/app.hpp"
#include "ebb/EbbManager/PrimitiveEbbManager.hpp"
#include "ebb/EventManager/SimpleEventManager.hpp"
#include "ebb/MessageManager/MPIMessageManager.hpp"
#include "ebb/HashTable/DistributedHashTable.hpp"
#include "ebbrt.hpp"

constexpr ebbrt::app::Config::InitEbb init_ebbs[] =
{
  {
    .create_root = ebbrt::PrimitiveEbbManagerConstructRoot,
    .name = "EbbManager"
  },
  {
    .create_root = ebbrt::SimpleEventManager::ConstructRoot,
    .name = "EventManager"
  },
  {
    .create_root = ebbrt::DistributedHashTable::ConstructRoot,
    .name = "HashTable"
  },
  {
    .create_root = ebbrt::MPIMessageManager::ConstructRoot,
    .name = "MessageManager"
  }
};

constexpr ebbrt::app::Config::StaticEbbId static_ebbs[] = {
  {.name = "EbbManager", .id = 2},
  {.name = "EventManager", .id = 5},
  {.name = "HashTable", .id = 6},
  {.name = "MessageManager", .id = 7}
};

const ebbrt::app::Config ebbrt::app::config = {
  .space_id = 1,
  .num_init = sizeof(init_ebbs) / sizeof(Config::InitEbb),
  .init_ebbs = init_ebbs,
  .num_statics = sizeof(static_ebbs) / sizeof(Config::StaticEbbId),
  .static_ebb_ids = static_ebbs
};

int main(int argc, char** argv)
{
  if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
    std::cerr << "MPI_Init failed" << std::endl;
    return -1;
  }

  ebbrt::EbbRT instance;

  ebbrt::Context context{instance};
  context.Activate();

  ebbrt::message_manager->StartListening();
  if (MPI_Barrier(MPI_COMM_WORLD) != MPI_SUCCESS) {
    std::cerr << "MPI_Barrier failed" << std::endl;
    return -1;
  }
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (rank == 0) {
    ebbrt::hashtable->Set("foo", strlen("foo") + 1,
                          "bar", strlen("bar") + 1);

    ebbrt::hashtable->Get("foo", strlen("foo") + 1,
                          [](const char* val, size_t val_size) {
                            if (val == nullptr) {
                              std::cout << "Key not found" << std::endl;
                            } else {
                              std::cout << "Value found: " << val << std::endl;
                            }
                          });
    ebbrt::hashtable->SyncGet("asdf", strlen("asdf") + 1, 2,
                              [](const char* val, size_t val_size) {
                                std::cout << "Value found: " << val << std::endl;
                              });
    ebbrt::hashtable->Increment("foofoo", strlen("foofoo") + 1,
                                [](uint32_t val) {
                                  std::cout << "Got value: " << val << std::endl;
                                });
  }

  if (MPI_Barrier(MPI_COMM_WORLD) != MPI_SUCCESS) {
    std::cerr << "MPI_Barrier failed" << std::endl;
    return -1;
  }

  if (rank == 1 || rank == 2) {
    ebbrt::hashtable->SyncSet("asdf", strlen("asdf") + 1,
                              "fdsa", strlen("fdsa") + 1, 1);
  }

  context.Loop(-1);

  return 0;
}
