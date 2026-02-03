#pragma once

#include <cstdint>
#include <string>
#include <algorithm>

std::uint64_t Random();
std::string RandomCharset(size_t length);
void RandomCharset(std::string &output);
