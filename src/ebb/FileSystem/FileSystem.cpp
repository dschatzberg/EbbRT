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
#include "ebb/SharedRoot.hpp"
#include "ebb/FileSystem/FileSystem.hpp"
#include "ebb/MessageManager/MessageManager.hpp"

#ifdef __linux__
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

ebbrt::EbbRoot*
ebbrt::FileSystem::ConstructRoot()
{
  return new SharedRoot<FileSystem>;
}

ebbrt::FileSystem::FileSystem() : op_id_{0}
{
}

namespace {
  class FSHeader {
  public:
    unsigned op_id;
    enum {
      READ,
      READ_RETURN
    } op;
    uint64_t length;
    uint64_t offset;
  };
}

void
ebbrt::FileSystem::ReadFile(const char* name,
                            std::function<void(const char*, uint64_t)> cb,
                            uint64_t length,
                            uint64_t offset)
{
#ifdef __linux__
  assert(0);
#elif __ebbrt__
  //FIXME: should check length restrictions
  auto header = new FSHeader;
  unsigned op_id = op_id_.fetch_add(1, std::memory_order_relaxed);
  header->op_id = op_id;
  header->op = FSHeader::READ;
  header->length = length;
  header->offset = offset;
  BufferList list {
    std::pair<const void*, size_t>(header, sizeof(FSHeader)),
      std::pair<const void*, size_t>(name, strlen(name) + 1)};
  NetworkId id;
  id.mac_addr[0] = 0xff;
  id.mac_addr[1] = 0xff;
  id.mac_addr[2] = 0xff;
  id.mac_addr[3] = 0xff;
  id.mac_addr[4] = 0xff;
  id.mac_addr[5] = 0xff;
  lock_.Lock();
  cb_map_[op_id] = cb;
  lock_.Unlock();
  message_manager->Send(id, file_system, std::move(list),
                        [=]() {
                          free(header);
                        });
#endif
}

namespace {
  using namespace ebbrt;
  void handle_read(const NetworkId& from,
                   const char* buf,
                   size_t len)
  {
#ifdef __linux__
    auto path = buf + sizeof(FSHeader);
    auto fd = open(path, O_RDONLY);
    if (fd == -1) {
      throw std::runtime_error("Open failed");
    }
    auto header = reinterpret_cast<const FSHeader*>(buf);
    auto newbuf = new char[header->length];
    auto read_ret = read(fd, newbuf, header->length);
    close(fd);
    if (read_ret == -1) {
      throw std::runtime_error("Read failed");
    }
    auto newheader = new FSHeader;
    newheader->op_id = header->op_id;
    newheader->length = read_ret;
    newheader->op = FSHeader::READ_RETURN;
    BufferList list {
      std::pair<const void*, size_t>(newheader, sizeof(FSHeader)),
        std::pair<const void*, size_t>(newbuf, read_ret)};
    message_manager->Send(from, file_system, std::move(list),
                          [=]() {
                            free(newbuf);
                            free(newheader);
                          });
#elif __ebbrt__
    LRT_ASSERT(0);
#endif
  }
}
void
ebbrt::FileSystem::HandleReadComplete(const NetworkId& from,
                                      const char* buf,
                                      size_t len)
{
#ifdef __linux__
  assert(0);
#elif __ebbrt__
  auto header = reinterpret_cast<const FSHeader*>(buf);
  lock_.Lock();
  auto it = cb_map_.find(header->op_id);
  if (it != cb_map_.end()) {
    auto f = it->second;
    cb_map_.erase(header->op_id);
    lock_.Unlock();
    f(buf + sizeof(FSHeader), len - sizeof(FSHeader));
  } else {
    lock_.Unlock();
  }
#endif
}

void
ebbrt::FileSystem::HandleMessage(NetworkId from,
                                 const char* buf,
                                 size_t len)
{
#ifdef __linx__
  assert(len >= sizeof(FSHeader));
#elif __ebbrt__
  LRT_ASSERT(len >= sizeof(FSHeader));
#endif
  auto header = reinterpret_cast<const FSHeader*>(buf);
  switch (header->op) {
  case FSHeader::READ:
    handle_read(from, buf, len);
    break;
  case FSHeader::READ_RETURN:
    HandleReadComplete(from, buf, len);
    break;
  }
}
