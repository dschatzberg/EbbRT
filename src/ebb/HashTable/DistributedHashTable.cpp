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

#include "ebb/SharedRoot.hpp"
#include "ebb/HashTable/DistributedHashTable.hpp"

ebbrt::DistributedHashTable::DistributedHashTable(){
  /* If non-initialized, the hashtable will act single-node*/
  myid_ = 0;
  nodecount_ = 1;
}

ebbrt::EbbRoot*
ebbrt::DistributedHashTable::ConstructRoot()
{
  return new SharedRoot<DistributedHashTable>;
}

ebbrt::HashTable::mapped_t
ebbrt::DistributedHashTable::Get(key_t key)
{
  auto hasher = table_.hash_function();
  if(local(home(hasher(key)))){
    auto it = table_.find(std::string(key));
    if (it == table_.end()) 
      return "KEYNOTFOUND"; // FIXME pass-by-value, int return 
    else
      return it->second;
  }
  else
    std::cout << "DEBUG REMOTE GET" << home(hasher(key)) << "\n";
  return "REMOTE_ERROR";
}

int
ebbrt::DistributedHashTable::Init(id_t myid, unsigned int nodecount)
{
  myid_ = myid;
  nodecount_ = nodecount;
  return 0;
}

int
ebbrt::DistributedHashTable::Set(key_t key, mapped_t val)
{
  auto hasher = table_.hash_function();
  if(local(home(hasher(key))))
    table_[key] = val;
  else
    std::cout << "DEBUG REMOTE SET" << home(hasher(key)) << "\n";
  return 0;
}

