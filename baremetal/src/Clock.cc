//          Copyright Boston University SESA Group 2013 - 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
#include <ebbrt/Clock_internal.h>

#include <ebbrt/Cpuid.h>
#include <ebbrt/Debug.h>
#include <ebbrt/PitClock.h>
#include <ebbrt/PvClock.h>

namespace {
ebbrt::clock::Clock* the_clock;
}  // namespace

void ebbrt::clock::Init() {
  if (cpuid::features.kvm_clocksource2) {
    the_clock = PvClock::GetClock();
  } else {
    the_clock = PitClock::GetClock();
  }
}

void ebbrt::clock::ApInit() { the_clock->ApInit(); }

ebbrt::clock::Wall::time_point ebbrt::clock::Wall::Now() noexcept {
  return the_clock->Now();
}

std::chrono::nanoseconds ebbrt::clock::Uptime() noexcept {
  return the_clock->Uptime();
}

std::chrono::nanoseconds ebbrt::clock::TscToNano(uint64_t tsc) noexcept {
  return the_clock->TscToNano(tsc);
}
