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
#include "app/app.hpp"
#include "ebb/EbbManager/EbbManager.hpp"
#include "ebb/Ethernet/RawSocket.hpp"
#include "ebb/MessageManager/MessageManager.hpp"

using namespace ebbrt;

void
app::start()
{
  ethernet = EbbRef<Ethernet>(ebb_manager->AllocateId());
  ebb_manager->Bind(RawSocket::ConstructRoot, ethernet);
  message_manager->StartListening();

  // uint8_t frame[] = {
  //   0x52, 0x54, 0x00, 0x12, 0x34, 0x56,
  //   0xb2, 0xa8, 0x5a, 0x8a, 0xa8, 0x00,
  //   0x88, 0x12,
  //   0x74, 0x65, 0x73, 0x74, 0x00
  // };
  // BufferList buffers;
  // buffers.emplace_front(frame, sizeof(frame));
  // ethernet->Send(buffers);
}
