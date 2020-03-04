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


#include "clipp.h"

#include <string>
#include <filesystem>
#include <iostream>

using std::string;
using namespace clipp;

int main(int argc, char* argv[])
{
    bool full = true, incremental = false, details = false, tag = false, extract = false;
    string volume = "c:\\", path = "snap";
    uint64_t buf = 16, thread = 8;

    auto cli = (
        opt_value("volume", volume),
        opt_value("path", path),
        opt_value("buffer size (MB)", buf),
        opt_value("thread count", thread),
        option("-t", "--tag").set(tag).doc("Generate Human Readable .tag file"),
        option("-e", "--extract").set(extract).doc("Extract MFT"),
        option("-f", "--full").set(full).doc("Take base image"),
        option("-i", "--incremental").set(incremental).doc("Take incremental image"),
        option("-d", "--details").set(details).doc("Print Details")
        );

    try
    {
        if (!parse(argc, argv, cli)) std::cout << make_man_page(cli, argv[0]);
        else
        {

        }
    }
    catch (const std::exception & ex)
    {
        std::cerr << ex.what() << std::endl;
        return -1;
    }

    return 0;
}


#endif //! defined(BENCHMARK_RUNNER) && ! defined(TEST_RUNNER)


