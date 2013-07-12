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
  
  typedef std::string key_t;
  typedef std::string mapped_t;
  typedef size_t hash_t;

  class HashTable : public EbbRep {
  public:
    virtual mapped_t Get(key_t key) = 0;
    virtual int Set(key_t key, mapped_t val) = 0;
    virtual int Flush() = 0;
    virtual int Free(key_t key) = 0;
  };
}
#endif
