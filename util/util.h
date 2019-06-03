#ifndef _UTIL_H_
#define _UTIL_H_

#include <vector>
#include <string>
#include <sstream>
#include <stdexcept>


// ("",  '.') -> [""]
// ("11", '.') -> ["11"]
// ("..", '.') -> ["", "", ""]
// ("11.", '.') -> ["11", ""]
// (".11", '.') -> ["", "11"]
// ("11.22", '.') -> ["11", "22"]
std::vector<std::string> split(const std::string &str, char d);

std::string toUpper(const std::string& s);

std::string trimLeft(const std::string& s, const std::string& cutSet = " \t\r\n");
std::string trimRight(const std::string& s, const std::string& cutSet = " \t\r\n");
std::string trim(const std::string& s, const std::string& cutSet = " \t\r\n");

namespace fmt
{
template<typename... Args>
std::string sprintf(const std::string& format, Args... args)
{
    int argc = sizeof...(args);
    std::vector<std::string> argv;
    argv.reserve(argc);

    auto toString = [](auto arg) -> std::string {
            std::ostringstream conv;
            conv << arg;
            return conv.str();
    };

    ((argv.emplace_back(toString(args)), ...));

    std::ostringstream result;

    std::size_t start = 0;
    int i = 0;
    for(auto nextValPos = format.find("%v"); 
        nextValPos != std::string::npos; 
        nextValPos = format.find("%v", start))
    {
        if (i >= argc) {
            throw std::invalid_argument("fmt::sprintf: extra value (%v)");
        }

        result << format.substr(start, nextValPos - start) << argv[i++];
        start = nextValPos + 2;
    }

    if (start < format.size()) {
        result << format.substr(start, format.size() - start);
    }

    if (i != argc) {
        throw std::invalid_argument("fmt::sprintf: extra argument");
    }

    return result.str();
}

} // namespace fmt

#endif