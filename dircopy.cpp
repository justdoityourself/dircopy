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

#ifdef _WIN32

#include "volsnap/snapshot.hpp"

#endif

#include "volstore/image.hpp"
#include "volstore/binary.hpp"

#include "clipp.h"

#include <string>
#include <filesystem>
#include <iostream>
#include <thread>
#include <filesystem>

#include "d8u/string_switch.hpp"
#include "d8u/util.hpp"
#include "d8u/transform.hpp"
#include "dircopy/backup.hpp"

using std::string;
using namespace clipp;
using d8u::switch_t;
using namespace dircopy;

int main(int argc, char* argv[])
{
    bool vss = false, recursive = true;
    string path = "", snapshot = "snap", image = "", action = "backup", key = "";
    size_t threads = 8;
    size_t files = 4;
    size_t compression = 5; //TODO enable
    size_t block_grouping = 16; //TODO enable
    auto domain = d8u::util::default_domain; //TODO enable

    auto cli = (
        option("-k", "--key").doc("The store key used to restore, mount or validate") & value("key", key),
        option("-a", "--action").doc("What action will be taken, backup, validate, delta, search, restore, fetch, enumerate") & value("action", action),
        option("-s", "--snapshot").doc("A path where snapshot databases are stored") & value("snapshot", path),
        option("-i", "--image").doc("Path, hostname or IP of the image or store: D:\\Backup, backup.com, 192.168.4.14") & value("image", image),
        option("-p", "--path").doc("Path to backup") & value("directory", path),
        option("-t", "--threads").doc("Threads used to encode / decode") & value("threads", threads),
        option("-f", "--files").doc("Files processed at a time") & value("threads", threads),
        option("-v", "--vss").doc("Use vss snapshot").set(vss),
        option("-r", "--recursive").doc("Recursive enumeration of directoroes").set(recursive)
        );

    bool running = true;
    d8u::util::Statistics stats;
    std::string current_file;

    std::thread console([&]()
    {

    });

    try
    {
        if (!parse(argc, argv, cli) || !image.size()) 
            std::cout << make_man_page(cli, argv[0]);
        else
        {
            d8u::transform::DefaultHash result;

            auto on_file = [&](auto size, auto time, auto name)
            {
                current_file = name;
            };

            auto do_switch = [&](auto& store)
            {
                switch (switch_t(action))
                {
                case switch_t("backup"):
                    if (vss)
                    {
#ifdef _WIN32
                        if (recursive)
                            result = backup::vss_folder2(snapshot, stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024);
                        else
                            result = backup::vss_single2(snapshot, stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024);
#else
                        std::cout << "VSS Not available on non-windows platform." << std::endl;
#endif
                    }
                    else
                    {
                        if (recursive)
                            result = backup::recursive_folder2(snapshot, stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024);
                        else
                            result = backup::single_folder2(snapshot, stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024);
                    }

                    break;
                }
            };
            
            if (std::filesystem::exists(image))
            {
                volstore::Image store(image);
                do_switch(store);
            }
            else
            {
                volstore::BinaryStoreClient store(image + ":9009", image + ":1010", image + ":1111");
                do_switch(store);
            }

            running = false;
            console.join();
            std::cout << "result todo" << std::endl;
            std::cout << "Finished in " << "TODO" << std::endl;
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


