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
#include "ebb/EventManager/SimpleEventManager.hpp"
#ifdef __ebbrt__
#include "lrt/bare/assert.hpp"
#endif

ebbrt::EbbRoot*
ebbrt::SimpleEventManager::ConstructRoot()
{
  return new SharedRoot<SimpleEventManager>();
}

ebbrt::SimpleEventManager::SimpleEventManager() : next_{32}
{
  //get the epoll fd for the event loop
  epoll_fd_ = epoll_create(1);
  if (epoll_fd_ == -1) {
    throw std::runtime_error("epoll_create failed");
  }
}

uint8_t
ebbrt::SimpleEventManager::AllocateInterrupt(std::function<void()> func)
{
  lock_.Lock();
  uint8_t ret = next_++;
  map_.insert(std::make_pair(ret, std::move(func)));
  lock_.Unlock();
  return ret;
}

void
ebbrt::SimpleEventManager::HandleInterrupt(uint8_t interrupt)
{
  lock_.Lock();
#ifdef __linux__
  assert(map_.find(interrupt) != map_.end());
#elif __ebbrt__
  LRT_ASSERT(map_.find(interrupt) != map_.end());
#endif
  const auto& f = map_[interrupt];
  lock_.Unlock();
  if (f) {
    f();
  }
}

void
ebbrt::SimpleEventManager::ProcessEvent()
{
#ifdef __linux__
  struct epoll_event epoll_event;
  //blocks until an event is ready
  while (epoll_wait(epoll_fd_, &epoll_event, 1, -1) == -1) {
    if (errno == EINTR) {
      continue;
    }
    throw std::runtime_error("epoll_wait failed");
  }
  ebbrt::lrt::event::_event_interrupt(epoll_event.data.u32);
#elif __ebbrt__
#endif
}

#ifdef __linux__
void
ebbrt::SimpleEventManager::RegisterFD(int fd,
                                      uint32_t events,
                                      uint8_t interrupt)
{
  struct epoll_event event;
  event.events = EPOLLIN;
  event.data.u32 = interrupt;
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) == -1) {
    throw std::runtime_error("epoll_ctl failed");
  }
}
#endif
