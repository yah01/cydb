#include <iostream>
#include "boost/asio.hpp"

namespace asio = boost::asio;
using tcp = asio::ip::tcp;



class Server
{
public:
    Server(asio::io_context &ctx)
        : io_ctx(ctx),
          acceptor(ctx, tcp::endpoint(tcp::v4(), 9595))
    {
        start();
    }

private:
    void start()
    {
        tcp::socket socket(io_ctx);
        acceptor.async_accept(socket, [this, &socket](boost::system::error_code ec) {
            std::cout << "async_accept -> " << ec.message() << std::endl;
            if (!ec)
            {
                std::cout << "connected" << std::endl;

                socket.write_some(
                    asio::buffer("hello"));

                sleep(5000);
                start();
            }
            socket.close();
        });
    }

    // void handle_accept(tcp::socket socket)
    // {
    // }

    asio::io_context &io_ctx;
    tcp::acceptor acceptor;
};