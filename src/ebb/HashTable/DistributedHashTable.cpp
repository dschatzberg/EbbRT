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

#include "ebb/SharedRoot.hpp"
#include "ebb/HashTable/DistributedHashTable.hpp"

ebbrt::EbbRoot*
ebbrt::DistributedHashTable::ConstructRoot()
{
  return new SharedRoot<DistributedHashTable>;
}

ebbrt::mapped_t
ebbrt::DistributedHashTable::Get(key_t key)
{
  auto it = table_.find(std::string(key));
  if (it == table_.end()) {
     std::cout <<  "NOKEY!";
    exit(1); // FIXME - passbyvalue, change return type 
  }

  return it->second;
}

int
ebbrt::DistributedHashTable::Set(key_t key, mapped_t val)
{
  table_[key] = val;
  return 0;
}

int
ebbrt::DistributedHashTable::Flush()
{
  table_.clear();
  return 0;
}

int
ebbrt::DistributedHashTable::Free(key_t key)
{
  table_.erase(key);
  return 0; 
}
