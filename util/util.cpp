#include <vector>
#include <string>
#include <algorithm>
#include <cctype>

#include "util.h"

std::vector<std::string> split(const std::string &str, char d)
{
    std::vector<std::string> r;

    std::string::size_type start = 0;
    std::string::size_type stop = str.find_first_of(d);
    while(stop != std::string::npos)
    {
        r.push_back(str.substr(start, stop - start));
        start = stop + 1;
        stop = str.find_first_of(d, start);
    }

    r.push_back(str.substr(start));

    return r;
}

std::string toUpper(const std::string& s)
{
    std::string ret;
    ret.resize(s.size());
    std::transform(s.begin(), s.end(), ret.begin(), ::toupper);
    return ret;
}

std::string trimLeft(const std::string& s, const std::string& cutSet)
{
    auto begin = s.find_first_not_of(cutSet);
    return s.substr(begin);
}

std::string trimRight(const std::string& s, const std::string& cutSet)
{
    auto end = s.find_last_not_of(cutSet);
    return s.substr(0, end + 1);
}

std::string trim(const std::string& s, const std::string& cutSet)
{
    auto begin = s.find_first_not_of(cutSet);
    auto end   = s.find_last_not_of(cutSet);
    return s.substr(begin, end - begin + 1);
}