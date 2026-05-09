#pragma once

#include "protocol/request.hpp"
#include "protocol/response.hpp"

class Broker {
  public:
    auto handle(const Request& req) -> Response;
};
