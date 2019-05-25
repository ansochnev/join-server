#include "protocol.h"
#include <sstream>

class Session : public std::enable_shared_from_this<Session>,
                public proto::IResponseWriter
{
    boost::asio::ip::tcp::socket m_socket;
    char m_buf[1024];

    proto::IHandler *m_handler;

    std::ostringstream m_response;
    std::string        m_responseStatus;

public:
    Session(boost::asio::ip::tcp::socket sock, proto::IHandler* handler) 
        : m_socket(std::move(sock)), m_handler(handler) {}

    ~Session() {
        m_socket.close();
    }

    void start() {
        recv();
    }

    void writeError(const std::string& message) override {
        m_responseStatus = "ERR " + message;
    }

    void write(const std::string& data) override {
        m_response << data;
    }

private:
    void recv()
    {
        auto self(shared_from_this());
        m_socket.async_read_some(boost::asio::buffer(m_buf, 1024),
            [this, self](const boost::system::error_code& err, std::size_t n)
            {
                if (!err) {
                    m_response.str("");
                    m_responseStatus = "OK";
                    
                    proto::Request req{std::string(m_buf, n)};
                    m_handler->handle(this, req);

                    if (!m_response.str().empty() && 
                        m_response.str().back() != '\n') 
                    {
                        m_response << '\n';
                    }
                    m_response << m_responseStatus << std::endl;

                    send(m_response.str());
                }
        });
    }

    void send(std::string&& s)
    {   
        auto self(shared_from_this());
        boost::asio::async_write(m_socket, boost::asio::buffer(s),
            [this, self](boost::system::error_code err, std::size_t /*length*/)
            {
                if (!err) {
                    recv();
                }
            });
    }
};


proto::Server::Server(short port, proto::IHandler* handler) 
    : 
    m_service(boost::asio::io_service()),
    m_acceptor(m_service, 
                    boost::asio::ip::tcp::endpoint(
                        boost::asio::ip::tcp::v4(), port)), 
    m_socket(m_service),
    m_handler(handler)
{
}

proto::Server::~Server() 
{
    m_socket.close();
    delete m_handler;
}

void proto::Server::run() 
{
    do_accept();
    m_service.run();
}

void proto::Server::do_accept()
{
    m_acceptor.async_accept(m_socket, 
        [this](const boost::system::error_code& err) 
        {
            if (err) {
                return;
            }
            std::make_shared<Session>(std::move(m_socket), m_handler)->start();
            this->do_accept();
        });
}