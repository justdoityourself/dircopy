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
#include "volstore/api.hpp"



#include "clipp.h"

#include <string>
#include <filesystem>
#include <iostream>
#include <thread>
#include <filesystem>

#include "d8u/string_switch.hpp"
#include "d8u/util.hpp"
#include "d8u/string.hpp"
#include "d8u/transform.hpp"
#include "d8u/json.hpp"

#include "dircopy/backup.hpp"
#include "dircopy/validate.hpp"
#include "dircopy/mount.hpp"

using std::string;
using namespace clipp;
using d8u::switch_t;
using namespace dircopy;
using namespace volstore::api;

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
    d8u::transform::DefaultHash key;

    bool vss = false, recursive = true, storage_server = false, scope = false;
    string path = "", snapshot = "", host = "", image = "", action = "backup", skey = "", dest = "", json = "", sdomain="";
    size_t threads = 8;
    size_t files = 4;
    size_t net_buffer = 16 * 1024 * 1024;
    bool validate = false;

    size_t compression = 5;
    size_t block_grouping = 16;
    std::vector<uint8_t> domain;

    auto cli = (
        option("-c", "--config").doc("Json configuration file") & value("json", json),
        option("-k", "--key").doc("The store key used to restore, mount or validate") & value("key", skey),
        option("-a", "--action").doc("What action will be taken, backup, validate_deep, validate, delta, search, restore, fetch, enumerate") & value("action", action),
        option("-s", "--snapshot").doc("A path where snapshot databases are stored") & value("snapshot", snapshot),
        option("-i", "--image").doc("Path of the image: D:\\Backup") & value("image", image),
        option("-h", "--host").doc("Hostname or IP of  store: backup.com, 192.168.4.14") & value("host", host),
        option("-p", "--path").doc("Path to backup") & value("directory", path),
        option("-d", "--destination").doc("Path to backup") & value("destination", dest),
        option("-o", "--domain").doc("Block identification salt") & value("domain", sdomain),
        option("-t", "--threads").doc("Threads used to encode / decode") & value("threads", threads),
        option("-nb", "--netbuffer").doc("Size of the TCP socket buffer") & value("network buffer", net_buffer),
        option("-b", "--blockgroup").doc("Group size of identification query") & value("block_grouping", block_grouping),
        option("-m", "--compression").doc("Compression Level ( 0 - 9 )") & value("compression", compression),
        option("-f", "--files").doc("Files processed at a time") & value("threads", files),
        option("-v", "--vss").doc("Use vss snapshot").set(vss),
        option("-p", "--scope").doc("Use vss snapshot").set(scope),
        option("-z", "--server").doc("Host block storage server").set(storage_server),
        option("-x", "--validate").doc("Validate Blocks that are read or restored").set(validate),
        option("-r", "--recursive").doc("Recursive enumeration of directories").set(recursive)
        );

    bool running = true;
    d8u::util::Statistics _stats;
    d8u::util::Statistics* pstats = &_stats;
    StorageService* pservice = nullptr;
    volstore::BinaryStoreClient* pclient = nullptr;
    std::string current_file,pk;

    auto start = std::chrono::high_resolution_clock::now();

    std::thread console([&]()
    {
        while (running)
        {
            pstats->Print();

            if(pservice)
                std::cout << "[ C: " << pservice->ConnectionCount() 
                << " M: " << pservice->MessageCount() 
                << " S: " << pservice->EventsStarted() 
                << " F: " << pservice->EventsFinished() 
                << " R: " << pservice->ReplyCount()  << " ]\r" << std::flush;
            else if(pclient)
                std::cout << "[ W: " << pclient->Writes() << " R: " << pclient->Reads() << " ] " << current_file << "\t\t\r" << std::flush;
            else
                std::cout << current_file << "\t\t\r" << std::flush;

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    });

    try
    {
        if (!parse(argc, argv, cli))
        {
            std::cout << make_man_page(cli, argv[0]);
            throw std::runtime_error("Bad CLI");
        }

        if (json.size() && std::filesystem::exists(json))
        {
            d8u::json::JsonMap map(json);

            map.ForEachValue([&](auto key, auto value)
            {
                switch(switch_t(key))
                {
                case switch_t("vss"):   vss = value;    break;
                case switch_t("key"):   skey = value;   break;
                case switch_t("host"):  host = value;   break;
                case switch_t("path"):  path = value;   break;
                case switch_t("files"):     files = value;      break;
                case switch_t("scope"):     scope = value;      break;
                case switch_t("image"):     image = value;      break;
                case switch_t("action"):    action = value;     break;
                case switch_t("domain"):    sdomain = value;    break;
                case switch_t("threads"):   threads = value;    break;
                case switch_t("snapshot"):      snapshot = value;       break;
                case switch_t("validate"):      validate = value;       break;
                case switch_t("netbuffer"):     net_buffer = value;     break;
                case switch_t("recursive"):     recursive = value;      break;
                case switch_t("blockgroup"):    block_grouping = value; break;
                case switch_t("destination"):   dest = value;           break;
                case switch_t("compression"):   compression = value;    break;
                }
            });
        }

        if (!image.size() && !host.size() && !storage_server )
            std::cout << make_man_page(cli, argv[0]);
        else
        {
            if (scope && path.size() && snapshot.size())
            {
                if (recursive)
                    _stats.direct.target = backup::recursive_delta(json, snapshot, path, [](auto name, auto size, auto time) { return true; });
                else
                    _stats.direct.target = backup::single_delta(json, snapshot, path, [](auto name, auto size, auto time) { return true;  });
            }

            d8u::transform::DefaultHash result;

            auto on_file = [&](auto & name, auto size, auto time)
            {
                auto pos1 = name.rfind("/");
                if (pos1 == std::string::npos)
                    pos1 = 0;

                auto pos2 = name.rfind("\\");
                if (pos2 == std::string::npos)
                    pos2 = 0;

                auto pos = (pos1 > pos2) ? pos1 : pos2;

                current_file = name.substr(pos+1);

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
                            result = backup::vss_folder2(json,snapshot, _stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024);
                        }
                        else
                        {
                            std::cout << "VSS Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                            result = backup::vss_single2(json,snapshot, _stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024);
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
                            result = backup::recursive_folder2(json,snapshot, _stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024);
                        }
                        else
                        {
                            std::cout << "Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                            result = backup::single_folder2(json,snapshot, _stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024);
                        }
                    }

                    pk = d8u::util::to_hex(result);

                    break;
                case switch_t("validate"):

                    std::cout << "Validate Directory: " << " Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    if (validate::folder2(_stats,key, store, domain, 1024 * 1024, 128 * 1024 * 1024, threads, files))
                        std::cout << "Validation Success" << std::endl;
                    else
                        std::cout << "Error Detected" << std::endl;
                    break;
                case switch_t("validate_deep"):

                    std::cout << "Validate Directory Deep: " << " Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    if (validate::deep_folder2(_stats,key, store, domain, 1024 * 1024, 128 * 1024 * 1024, threads, files))
                        std::cout << std::endl << "Validation Success" << std::endl;
                    else
                        std::cout << std::endl << "Error Detected" << std::endl;
                    break;
                case switch_t("delta"):

                    running = false;
                    console.join();

                    if (recursive)
                        backup::recursive_delta(json,snapshot, path, [](auto name, auto size, auto time)
                        {
                            std::cout << name << " " << size << " bytes " << time << " changed" << std::endl;

                            return true;
                        });
                    else
                        backup::single_delta(json,snapshot, path, [](auto name, auto size, auto time)
                        {
                            std::cout << name << " " << size << " bytes " << time << " changed" << std::endl;

                            return true;
                        });

                    break;
                case switch_t("search"):
                {
                    std::cout << "Search: " << path << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    mount::Path handle(key,store,domain,validate);

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

                    restore::folder2(_stats, path, key, store, domain, validate, validate, 1024 * 1024, 128* 1024 * 1024, threads, files);

                    break;
                case switch_t("fetch"):
                {
                    std::cout << "Fetch: " << path << " >> " << dest << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    mount::Path handle(key, store, domain, validate);

                    running = false;
                    console.join();

                    handle.Fetch(path,dest ,threads);

                    handle.PrintUsage();
                }
                    break;
                case switch_t("enumerate"):
                {
                    std::cout << "Enumerate: " << " Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    mount::Path handle(key, store, domain, validate);

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

            if (storage_server)
            {
                StorageService service(path, threads);
                pservice = &service;

                pstats = service.Stats();

                service.Join();
            }
            else
            {
                if (sdomain.size())
                    domain = d8u::util::to_bin(sdomain);
                else
                {
                    domain.resize(d8u::util::default_domain.size());
                    std::copy(d8u::util::default_domain.begin(), d8u::util::default_domain.end(), domain.begin());
                }

                if (skey.size())
                {
                    auto vkey = d8u::util::to_bin(skey);
                    if (vkey.size() != 32)
                        throw std::runtime_error("Bad input key");

                    std::copy(vkey.begin(), vkey.end(), key.begin());
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
                    std::string query, read, write;

                    switch (switch_t(action))
                    {
                    default:
                    case switch_t("delta"):
                        break;
                    case switch_t("backup"):
                        query = host + ":9009";
                        write = host + ":1111";
                        break;
                    case switch_t("fetch"):
                    case switch_t("enumerate"):
                    case switch_t("search"):
                    case switch_t("validate_deep"):
                    case switch_t("restore"):
                        read = host + ":1010";
                        break;
                    case switch_t("validate"):
                        query = host + ":9009";
                        read = host + ":1010";
                        break;
                    }

                    volstore::BinaryStoreClient store(snapshot + "\\" + host + ".cache", query, read, write, net_buffer);
                    pclient = &store;
                    do_switch(store);
                }
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

    pstats->Print();

    if(pk.size()) std::cout << std::endl << std::endl << "Key: " << pk << std::endl << std::endl;

    std::chrono::duration<double> elapsed = std::chrono::high_resolution_clock::now() - start;

    std::cout << std::endl << "Finished in: " << elapsed.count() << "s" << std::endl;

    return 0;
}


#endif //! defined(BENCHMARK_RUNNER) && ! defined(TEST_RUNNER)


