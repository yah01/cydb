// #include <iostream>

// #include "boost/asio.hpp"
// #include "rocksdb/db.h"

// #include "server.hpp"
// #include "storage.hpp"
// #include "handlers.hpp"

// namespace asio = boost::asio;

// int main()
// {
// 	asio::io_context ctx;
// 	Server server(ctx);

// 	std::cout << "servering" << std::endl;

// 	ctx.run();
// }

//
// server.cpp
// ~~~~~~~~~~
//
// Copyright (c) 2003-2020 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <ctime>
#include <iostream>
#include <string>
#include <memory>
#include <cassert>

#include "engines/rocksdb.hpp"
#include "engines/btree/btree.hpp"

void test_rocksdb()
{
	std::unique_ptr<cyber::KvEngine> engine = std::make_unique<cyber::RocksDB>();
	auto s = engine->open("testdb");
	if (s.err != cyber::OpError::Ok)
	{
		std::cout << "can't open\n";
		exit(-1);
	}

	engine->set("hello", "world");
	s = engine->get("hello");
	std::cout << s.value << "\n";

	engine->remove("hello");
	s = engine->get("hello");
	assert(s.err == cyber::OpError::KeyNotFound);
}

void test_btree()
{
	cyber::KvEngine *engine = new cyber::BTree();
	auto s = engine->open("testdb");
	if (s.err != cyber::OpError::Ok)
	{
		std::cout << "can't open\n";
		exit(-1);
	}
	s = engine->get("hello");
	std::cout << s.value << "\n";

	s = engine->get("test_buf");
	std::cout << s.value << "\n";

	engine->set("hello", "world");
	s = engine->get("hello");
	std::cout << s.value << "\n";

	std::cout << "delete engine\n";

	delete engine;
	engine = new cyber::BTree();
	s = engine->open("testdb");
	if (s.err != cyber::OpError::Ok)
	{
		std::cout << "can't open\n";
		exit(-1);
	}

	s = engine->get("hello");
	std::cout << s.value << "\n";

	engine->set("test_buf", "in buffer");
	s = engine->get("test_buf");
	std::cout << s.value << "\n";

	s = engine->get("hello");
	std::cout << s.value << "\n";

	engine->remove("hello");
	s = engine->get("hello");
	if (s.err != cyber::OpError::KeyNotFound)
	{
		std::cout << "not right\n";
		exit(-1);
	}

	delete engine;
}

void test_btree_split()
{
	cyber::KvEngine *engine = new cyber::BTree();
	auto s = engine->open("testdb");
	if (s.err != cyber::OpError::Ok)
	{
		std::cout << "can't open\n";
		exit(-1);
	}

	for (int i = 0; i < 4096; i++)
	{
		
	}
}

int main()
{
	test_btree();
	return 0;
}