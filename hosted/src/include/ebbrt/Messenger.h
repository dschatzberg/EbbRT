//          Copyright Boston University SESA Group 2013 - 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
#ifndef HOSTED_SRC_INCLUDE_EBBRT_MESSENGER_H_
#define HOSTED_SRC_INCLUDE_EBBRT_MESSENGER_H_

#include <mutex>

#include <boost/asio.hpp>

#include <ebbrt/Buffer.h>
#include <ebbrt/EbbRef.h>
#include <ebbrt/Future.h>
#include <ebbrt/StaticIds.h>
#include <ebbrt/StaticSharedEbb.h>

namespace ebbrt {
class Messenger : public StaticSharedEbb<Messenger> {
 public:
  class NetworkId {
   public:
    explicit NetworkId(boost::asio::ip::address_v4 ip) : ip_(std::move(ip)) {}

   private:
    boost::asio::ip::address_v4 ip_;

    friend class Messenger;
  };

  Messenger();

  Future<void> Send(NetworkId to, std::shared_ptr<const Buffer> data);

 private:
  class Session {
   public:
    explicit Session(boost::asio::ip::tcp::socket socket);
    Future<void> Send(std::shared_ptr<const Buffer> data);

   private:
    struct Header {
      uint64_t size;
    };
    void ReadHeader();
    void ReadMessage();

    Header header_;
    boost::asio::ip::tcp::socket socket_;
  };

  void DoAccept();
  uint16_t GetPort();

  boost::asio::ip::tcp::acceptor acceptor_;
  boost::asio::ip::tcp::socket socket_;
  std::mutex m_;
  std::unordered_map<uint32_t, Future<Session>> connection_map_;

  friend class Session;
  friend class NodeAllocator;
};

const constexpr auto messenger = EbbRef<Messenger>(kMessengerId);
}  // namespace ebbrt

#endif  // HOSTED_SRC_INCLUDE_EBBRT_MESSENGER_H_
