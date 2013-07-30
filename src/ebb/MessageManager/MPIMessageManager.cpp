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
#include <cstdint>
#include <cstdlib>

#include "ebb/DistributedRoot.hpp"
#include "ebb/EventManager/EventManager.hpp"
#include "ebb/MessageManager/MPIMessageManager.hpp"

ebbrt::EbbRoot*
ebbrt::MPIMessageManager::ConstructRoot()
{
  return new DistributedRoot<MPIMessageManager>;
}

namespace {
  class MessageHeader {
  public:
    uint16_t context;
    ebbrt::EbbId ebb;
  };

  constexpr int MESSAGE_MANAGER_TAG = 0x88120000;
}

ebbrt::MPIMessageManager::MPIMessageManager()
{
}


void
ebbrt::MPIMessageManager::Send(NetworkId to,
                            EbbId id,
                            BufferList buffers,
                            std::function<void()> cb)
{
  size_t len = sizeof(MessageHeader);
  for (const auto& buffer : buffers) {
    len += buffer.second;
  }

  std::vector<char> buf(len);

  auto mh = reinterpret_cast<MessageHeader*>(buf.data());
  mh->context = lrt::event::get_location();
  mh->ebb = id;
  char* location = buf.data() + sizeof(MessageHeader);
  for (const auto& buffer : buffers) {
    memcpy(location, buffer.first, buffer.second);
    location += buffer.second;
  }

  bufs_.push_back(std::move(buf));
  MPI_Request req;
  if (MPI_Isend(bufs_.back().data(), len, MPI_CHAR,
                to.rank, MESSAGE_MANAGER_TAG | to.context, MPI_COMM_WORLD,
                &req) != MPI_SUCCESS) {
    throw std::runtime_error("MPI_Send failed");
  }
  reqs_.push_back(std::move(req));

  //FIXME: do this asynchronously
  cb();
}

void
ebbrt::MPIMessageManager::StartListening()
{
  interrupt_ = event_manager->AllocateInterrupt([]() {
      auto me = static_cast<EbbRef<MPIMessageManager> >(message_manager);
      me->DispatchMessage();
    });

  lrt::event::register_function([]() {
      auto me = static_cast<EbbRef<MPIMessageManager> >(message_manager);
      return me->CheckForInterrupt();
    });
}

int
ebbrt::MPIMessageManager::CheckForInterrupt()
{
  if (!reqs_.empty()) {
    int index;
    int flag;
    MPI_Status status;
    do {
      if (MPI_Testany(reqs_.size(), reqs_.data(), &index,
                      &flag, &status) != MPI_SUCCESS) {
        throw std::runtime_error("Testany failed");
      }
      if (flag) {
        reqs_.erase(reqs_.begin() + index);
        bufs_.erase(bufs_.begin() + index);
      }
    } while (flag && !reqs_.empty());
  }

  int flag;
  if (MPI_Iprobe(MPI_ANY_SOURCE, MESSAGE_MANAGER_TAG | get_location(),
                 MPI_COMM_WORLD,
                 &flag, &status_) != MPI_SUCCESS) {
    throw std::runtime_error("Iprobe failed");
  }

  if (flag) {
    return interrupt_;
  } else {
    return -1;
  }
}

void
ebbrt::MPIMessageManager::DispatchMessage()
{
  int len;
  if (MPI_Get_count(&status_, MPI_CHAR, &len) != MPI_SUCCESS) {
    throw std::runtime_error("MPI_Get_count failed");
  }
  char* buf = new char[len];
  MPI_Status status;
  if (MPI_Recv(buf, len, MPI_CHAR, MPI_ANY_SOURCE, MESSAGE_MANAGER_TAG,
               MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
    throw std::runtime_error("MPI_Recv failed");
  }

  auto mh = reinterpret_cast<const MessageHeader*>(buf);
  EbbRef<EbbRep> ebb {mh->ebb};
  NetworkId from;
  from.rank = status.MPI_SOURCE;
  from.context = mh->context;
  ebb->HandleMessage(from,
                     buf + sizeof(MessageHeader),
                     len - sizeof(MessageHeader));
  delete[] buf;
}
