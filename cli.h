#pragma once

#include <array>

int cli(int argc, char* argv[]);

template<typename... N> int cli2(N&&... args)
{
    std::array<const char*, sizeof...(args)> argv{ std::forward<N>(args)... };
    return cli((int)argv.size(), (char**)argv.data());
}
