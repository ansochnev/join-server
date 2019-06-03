#include "protocol.h"
#include "db.h"
#include "util/util.h"

class Joiner : public proto::IHandler
{
    sql::IDBConnection *m_conn;
public:
    Joiner(sql::IDBConnection* db) : m_conn(db) 
    {
        sql::IStatement *statement = m_conn->createStatement();
        statement->modify("CREATE TABLE A (id INTEGER PRIMARY KEY, name TEXT);");
        statement->modify("CREATE TABLE B (id INTEGER PRIMARY KEY, name TEXT);");
    }

    ~Joiner() = default;

    void handle(proto::IResponseWriter* rw, proto::Request& req) override
    {
        // Remove '\n'.
        std::string query = req.query.substr(0, req.query.size() - 1);
        std::string operation;

        auto pos = query.find(' ');
        if (pos != std::string::npos) {
            operation = query.substr(0, pos);
        } else {
            operation = query;
        }

        if (operation == proto::SHOW) {
            show(rw, query);
        }
        else if (operation == proto::INSERT) {
            insert(rw, query);
        }
        else if (operation == proto::TRUNCATE) {
            truncate(rw, query);
        }
        else if (query == proto::INTERSECTION) {
            intersection(rw);        
        }
        else if (query == proto::SYMDIFF) {
            symdiff(rw);
        } 
        else {
            rw->writeError("unknown operation '" + operation + "'");
        }
    }

private:
    void show(proto::IResponseWriter* rw, const std::string& query) 
    {
        auto tokens = split(query, ' ');
        if (tokens.size() != 2) {
            rw->writeError("bad request");
            return;
        }
        writeSelection(rw, fmt::sprintf("SELECT * FROM %v;", tokens[1]));
    }

    void insert(proto::IResponseWriter* rw, const std::string& query)
    {
        auto tokens = split(query, ' ');
        if (tokens.size() != 4) {
            rw->writeError("bad request");
            return;
        }
        
        std::string sqlQuery = fmt::sprintf("INSERT INTO %v VALUES (%v, \"%v\");",
                                            tokens[1], tokens[2], tokens[3]);
        
        sql::IStatement *statement = m_conn->createStatement();
        try {
            statement->modify(sqlQuery);
        }
        catch(sql::Exception& e) {
            rw->writeError(e.what());
        }
    }

    void truncate(proto::IResponseWriter* rw, const std::string& query)
    {
        auto tokens = split(query, ' ');
        if (tokens.size() != 2) {
            rw->writeError("bad request");
            return;
        }

        std::string sqlQuery = fmt::sprintf("DELETE FROM %v;", tokens[1]);
        
        sql::IStatement *statement = m_conn->createStatement();
        try {
            statement->modify(sqlQuery);
        }
        catch(sql::Exception& e) {
            rw->writeError(e.what());
        }
    }

    void intersection(proto::IResponseWriter* rw) {
        writeSelection(rw, "SELECT * FROM A JOIN B ON A.id = B.id");
    }

    void symdiff(proto::IResponseWriter* rw [[maybe_unused]])
    {
    }

    void writeSelection(proto::IResponseWriter* rw, const std::string& sqlQuery)
    {
        sql::IStatement *statement = m_conn->createStatement();
        sql::ISelection *selection = statement->select(sqlQuery);

        while(selection->hasNext()) 
        {
            selection->next();
            rw->write(fmt::sprintf("%v,%v,%v", 
                
                selection->isNull(0) ? "" 
                    : fmt::sprintf("%v", selection->getLong(0)),

                selection->isNull(1) ? "" 
                    : fmt::sprintf("%v", selection->getString(1))
            ));
        }
    }
};