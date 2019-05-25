#include "protocol.h"

class Handler : public proto::IHandler
{
public:
    ~Handler() = default;

    void handle(proto::IResponseWriter* rw, proto::Request& req) override
    {
        std::stringstream query(req.query);
        std::string operation;
        query >> operation;

        if (operation == proto::INSERT) {
            rw->write("insert");
        }
        else if (operation == proto::INTERSECTION) {
            rw->write("intersect");
        }
        else if (operation == proto::TRUNCATE) {
            rw->write("truncate");
        }
        else if (operation == proto::SYMDIFF) {
            rw->write("symdiff");
        } 
        else {
            rw->writeError("unknown operation");
        }
    }
};