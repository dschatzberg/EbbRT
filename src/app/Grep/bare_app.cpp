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
#include "ebb/PCI/PCI.hpp"
#include "ebb/Ethernet/VirtioNet.hpp"
#include "ebb/FileSystem/FileSystem.hpp"
#include "ebb/MessageManager/MessageManager.hpp"

using namespace ebbrt;

void
app::start()
{
  pci = EbbRef<PCI>(ebb_manager->AllocateId());
  ebb_manager->Bind(PCI::ConstructRoot, pci);
  ethernet = EbbRef<Ethernet>(ebb_manager->AllocateId());
  ebb_manager->Bind(VirtioNet::ConstructRoot, ethernet);

  message_manager->StartListening();

  file_system->ReadFile("/tmp/test",
                        [](const char* data, uint64_t len) {
                          lrt::console::write(data);
                        },
                        1024);
}
