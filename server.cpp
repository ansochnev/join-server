#include <iostream>
#include <memory>
#include <boost/asio.hpp>


class Session : public std::enable_shared_from_this<Session>
{
    boost::asio::ip::tcp::socket m_socket;
    char m_buf[1024];

public:
    Session(boost::asio::ip::tcp::socket sock) : m_socket(std::move(sock)) {}

    ~Session() 
    {
        m_socket.close();
    }

    void start() 
    {
        do_read();
    }

private:
    void do_read()
    {
        auto self(shared_from_this());
        m_socket.async_read_some(boost::asio::buffer(m_buf),
            [this, self](const boost::system::error_code& err, std::size_t n)
            {
                if (!err) {
                    do_write(n);
                }
            });
    }

    void do_write(std::size_t length)
    {
        auto self(shared_from_this());
        boost::asio::async_write(m_socket, boost::asio::buffer(m_buf, length),
            [this, self](boost::system::error_code err, std::size_t /*length*/)
            {
                if (!err) {
                    do_read();
                }
            });
    }

};

class Server
{
    boost::asio::ip::tcp::acceptor m_acceptor;
    boost::asio::ip::tcp::socket   m_socket;

public:
    Server(boost::asio::io_service& service, short port) 
        : 
        m_acceptor(service, 
                     boost::asio::ip::tcp::endpoint(
                         boost::asio::ip::tcp::v4(), port)), 
        m_socket(service)
    {
        do_accept();
    }

    ~Server()
    {
        m_socket.close();
    }

private:
    void do_accept()
    {
        m_acceptor.async_accept(m_socket, 
            [this](const boost::system::error_code& err) 
            {
                if (err) {
                    return;
                }
                std::make_shared<Session>(std::move(m_socket))->start();
                this->do_accept();
            });
    }
};

int main(int argc, char* argv[]) 
{
    if (argc != 2) {
        std::cout << "too few arguments" << std::endl;
        return 1;
    }

    try {
        int port = std::stoi(argv[1]);

        boost::asio::io_service service;
        Server server(service, port);
        service.run();
    }
    catch(std::exception& e) {
        std::cout << e.what();
    }

    return 0;
}
