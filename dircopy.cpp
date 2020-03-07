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
#include "dircopy/validate.hpp"
#include "dircopy/mount.hpp"

using std::string;
using namespace clipp;
using d8u::switch_t;
using namespace dircopy;

/*
    CLI:

    -i cli/image -a backup -p testdata -s cli/snap

        Recursive Directory Backup: testdata; State: cli/snap; Domain: c522acad91dd42c690c8ee60820b1879

        Key: e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511

        Threads 0, Files 0, Read 130940761, Write 92648156, Duplicate 0
        Finished in: 5.34995s



    -i cli/image -a validate_deep -k e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511

        Validate Directory Deep:  Domain: c522acad91dd42c690c8ee60820b1879

        Validation Success
        Threads 0, Files 0, Read 92648156, Write 0, Duplicate 0
        Finished in: 7.0275s



    -i cli/image -a validate -k e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511

        Validate Directory:  Domain: c522acad91dd42c690c8ee60820b1879

        Validation Success
        Threads 0, Files 0, Read 5045, Write 0, Duplicate 0
        Finished in: 1.0182s



    -i cli/image -a search -k e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511 -p compress

        Search: compress; Domain: c522acad91dd42c690c8ee60820b1879

        testdata\tiny_nocompress 141962 bytes 1583083122 changed
        testdata\tiny_compress 347136 bytes 1583083119 changed
        testdata\large_compress 93789184 bytes 1583083100 changed
        testdata\medium_nocompress 30510287 bytes 1583083110 changed
        testdata\small_compress 5103616 bytes 1583083116 changed
        Found 5 files

        Threads 0, Files 0, Read 5177, Write 0, Duplicate 0
        Finished in: 1.0238s



    -i cli/image -a restore -k e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511 -p cli/restore

        Restore: cli/restore; Domain: c522acad91dd42c690c8ee60820b1879

        Threads 0, Files 0, Read 92648288, Write 129892185, Duplicate 0
        Finished in: 2.34409s



    -i cli/image -a fetch -k e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511 -p testdata\tiny_compress -d cli/fetch

        Fetch: testdata\tiny_compress >> cli/fetch; Domain: c522acad91dd42c690c8ee60820b1879

        Threads 0, Files 0, Read 213013, Write 347136, Duplicate 0
        Finished in: 1.05452s



    -i cli/image -a enumerate -k e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511

        Enumerate:  Domain: c522acad91dd42c690c8ee60820b1879

        testdata\tiny_nocompress 141962 bytes 1583083122 changed
        testdata\tiny_compress 347136 bytes 1583083119 changed
        testdata\large_compress 93789184 bytes 1583083100 changed
        testdata\medium_nocompress 30510287 bytes 1583083110 changed
        testdata\small_compress 5103616 bytes 1583083116 changed
        Threads 0, Files 0, Read 5177, Write 0, Duplicate 0
        Finished in: 1.02567s



    -i cli/image -a delta -p testdata -s cli/snap

        testdata\New Text Document.txt 16 bytes 1583451225 changed

        Finished in: 1.02594s
*/

int main(int argc, char* argv[])
{
    std::vector<uint8_t> vkey;
    d8u::transform::DefaultHash dkey;

    bool vss = false, recursive = true;
    string path = "", snapshot = "", host = "", image = "", action = "backup", key = "", dest = "";
    size_t threads = 8;
    size_t files = 4;
    bool validate = false;
    size_t compression = 5; //TODO enable
    size_t block_grouping = 16; //TODO enable
    auto domain = d8u::util::default_domain; //TODO enable

    auto cli = (
        option("-k", "--key").doc("The store key used to restore, mount or validate") & value("key", key),
        option("-a", "--action").doc("What action will be taken, backup, validate_deep, validate, delta, search, restore, fetch, enumerate") & value("action", action),
        option("-s", "--snapshot").doc("A path where snapshot databases are stored") & value("snapshot", snapshot),
        option("-i", "--image").doc("Path of the image: D:\\Backup") & value("image", image),
        option("-h", "--host").doc("Hostname or IP of  store: backup.com, 192.168.4.14") & value("host", host),
        option("-p", "--path").doc("Path to backup") & value("directory", path),
        option("-d", "--destination").doc("Path to backup") & value("destination", dest),
        option("-t", "--threads").doc("Threads used to encode / decode") & value("threads", threads),
        option("-f", "--files").doc("Files processed at a time") & value("threads", threads),
        option("-v", "--vss").doc("Use vss snapshot").set(vss),
        option("-x", "--validate").doc("Use vss snapshot").set(validate),
        option("-r", "--recursive").doc("Recursive enumeration of directories").set(recursive)
        );

    bool running = true;
    d8u::util::Statistics stats;
    std::string current_file;

    auto start = std::chrono::high_resolution_clock::now();

    std::thread console([&]()
    {
        while (running)
        {
            std::cout << current_file << " ";

            stats.Print();

            std::cout << "\r";
            std::cout.flush();

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    });

    try
    {
        if (!parse(argc, argv, cli) || ( !image.size() && !host.size() ))
            std::cout << make_man_page(cli, argv[0]);
        else
        {
            d8u::transform::DefaultHash result;

            auto on_file = [&](auto & name, auto size, auto time)
            {
                current_file = name;

                return true;
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
                        {
                            std::cout << "VSS Recursive Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                            result = backup::vss_folder2(snapshot, stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024);
                        }
                        else
                        {
                            std::cout << "VSS Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                            result = backup::vss_single2(snapshot, stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024);
                        }
#else
                        std::cout << "VSS Not available on non-windows platform." << std::endl;
#endif
                    }
                    else
                    {
                        if (recursive)
                        {
                            std::cout << "Recursive Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                            result = backup::recursive_folder2(snapshot, stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024);
                        }
                        else
                        {
                            std::cout << "Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                            result = backup::single_folder2(snapshot, stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024);
                        }
                    }

                    std::cout << "Key: " << d8u::util::to_hex(result) << std::endl << std::endl;

                    break;
                case switch_t("validate"):

                    std::cout << "Validate Directory: " << " Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    if (validate::folder2(stats,dkey, store, domain, 1024 * 1024, 128 * 1024 * 1024, threads, files))
                        std::cout << "Validation Success" << std::endl;
                    else
                        std::cout << "Error Detected" << std::endl;
                    break;
                case switch_t("validate_deep"):

                    std::cout << "Validate Directory Deep: " << " Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    if (validate::deep_folder2(stats,dkey, store, domain, 1024 * 1024, 128 * 1024 * 1024, threads, files))
                        std::cout << std::endl << "Validation Success" << std::endl;
                    else
                        std::cout << std::endl << "Error Detected" << std::endl;
                    break;
                case switch_t("delta"):

                    running = false;
                    console.join();

                    if (recursive)
                        backup::recursive_delta(snapshot, path, [](auto name, auto size, auto time)
                        {
                            std::cout << name << " " << size << " bytes " << time << " changed" << std::endl;

                            return true;
                        });
                    else
                        backup::single_delta(snapshot, path, [](auto name, auto size, auto time)
                        {
                            std::cout << name << " " << size << " bytes " << time << " changed" << std::endl;

                            return true;
                        });

                    break;
                case switch_t("search"):
                {
                    std::cout << "Search: " << path << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    mount::Path handle(dkey,store,domain,validate);

                    running = false;
                    console.join();

                    auto count = handle.Search(path, [&](auto size, auto time, auto name, auto keys)
                    {
                        std::cout << name << " " << size << " bytes " << time << " changed" << std::endl;

                        return true;
                    });

                    std::cout << "Found " << count << " files" << std::endl << std::endl;

                    handle.PrintUsage();
                }
                    break;
                case switch_t("restore"):

                    std::cout << "Restore: " << path << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    std::filesystem::create_directories(path);

                    restore::folder2(stats, path, dkey, store, domain, validate, validate, 1024 * 1024, 128* 1024 * 1024, threads, files);

                    break;
                case switch_t("fetch"):
                {
                    std::cout << "Fetch: " << path << " >> " << dest << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    mount::Path handle(dkey, store, domain, validate);

                    running = false;
                    console.join();

                    handle.Fetch(path,dest ,threads);

                    handle.PrintUsage();
                }
                    break;
                case switch_t("enumerate"):
                {
                    std::cout << "Enumerate: " << " Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    mount::Path handle(dkey, store, domain, validate);

                    running = false;
                    console.join();

                    handle.Enumerate([&](auto size, auto time, auto name, auto keys)
                    {
                        std::cout << name << " " << size << " bytes " << time << " changed" << std::endl;

                        return true;
                    });

                    handle.PrintUsage();
                }
                    break;
                }
            };

            if (key.size())
            {
                vkey = d8u::util::to_bin(key);
                if (vkey.size() != 32)
                    throw std::runtime_error("Bad input key");

                std::copy(vkey.begin(), vkey.end(), dkey.begin());
            }
                

            if (snapshot.size())
                std::filesystem::create_directories(snapshot);

            if (image.size())
                std::filesystem::create_directories(image);
            
            if (image.size())
            {
                volstore::Image store(image);
                do_switch(store);
            }
            else
            {
                volstore::BinaryStoreClient store(host + ":9009", host + ":1010", host + ":1111");
                do_switch(store);
            }
        }
    }
    catch (const std::exception & ex)
    {
        std::cerr << std::endl << ex.what() << std::endl;
        return -1;
    }

    if (running)
    {
        running = false;
        console.join();
    }

    auto finish = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> elapsed = finish - start;

    stats.Print();

    std::cout << std::endl << "Finished in: " << elapsed.count() << "s" << std::endl;

    return 0;
}


#endif //! defined(BENCHMARK_RUNNER) && ! defined(TEST_RUNNER)


