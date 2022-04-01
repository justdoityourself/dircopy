/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#ifdef TEST_RUNNER


#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "dircopy/test.hpp"

int main(int argc, char* argv[])
{
    return Catch::Session().run(argc, argv);
}


#endif //TEST_RUNNER


#if ! defined(TEST_RUNNER)

#include "cli.h"

int main(int argc, char* argv[])
{
    return cli(argc, argv);
}


#endif //! defined(BENCHMARK_RUNNER) && ! defined(TEST_RUNNER)


