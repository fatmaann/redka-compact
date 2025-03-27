#include <iostream>
#include <string>
#include <map>
#include <regex>

auto parseRecordToMap(const std::string& str) {
    std::regex re(R"(\{([^}]+)\})");
    std::smatch match;

    std::map<std::string, std::string> result;
    if (regex_search(str, match, re)) {
        std::string recordContent = match[1].str();
        std::stringstream ss(recordContent);
        std::string keyValue;
        while (getline(ss, keyValue, ' ')) {
            size_t pos = keyValue.find(':');
            if (pos != std::string::npos) {
                std::string key = keyValue.substr(0, pos);
                key.erase(std::remove(key.begin(), key.end(), '{'), key.end());
                std::string value = keyValue.substr(pos + 1);
                result[key] = value;
            }
        }
    }
    return result;
}

std::string convertMapToRecord(const std::map<std::string, std::pair<uint32_t, std::string>>& map) {
    std::stringstream result;
    result << "{";
    bool first = true;
    for (const auto& kv : map) {
        if (!first) {
            result << " ";
        }
        first = false;
        // Omitting version for economy and readability
        if (kv.second.first == 1) {
            result << kv.first << ":" << kv.second.second;
        } else {
            result << kv.first << "@" << kv.second.first << ":" << kv.second.second;
        }
    }
    result << "}";
    return result.str();
}

void addToMergeMap(std::map<std::string, std::pair<uint32_t, std::string>> &mergeMap,
                   const std::map<std::string, std::string>& map) {
    for (const auto& kv : map) {
        uint32_t version = 1;
        std::string key = kv.first;
        size_t pos = key.find('@');
        if (pos != std::string::npos) {
            key = kv.first.substr(0, pos);
            version = static_cast<uint32_t>(std::stoul(kv.first.substr(pos + 1)));
        }

        if (mergeMap.find(key) != mergeMap.end()) {
            uint32_t previousVersion = mergeMap[key].first;
            if (previousVersion >= version)
                continue;
        }
        mergeMap[key] = std::make_pair(version, kv.second);
    }
}


auto mergeTwoMaps(std::map<std::string, std::string>& firstMap, std::map<std::string, std::string>& secondMap) {
    // Format: key - (version, value)
    std::map<std::string, std::pair<uint32_t, std::string>> mergeMap;
    addToMergeMap(mergeMap, firstMap);
    addToMergeMap(mergeMap, secondMap);
    return mergeMap;
}

std::string mergeTwoRecords(const std::string& firstRecord, const std::string& secondRecord) {
    std::map<std::string, std::string> firstMap = parseRecordToMap(firstRecord);
    std::map<std::string, std::string> secondMap = parseRecordToMap(secondRecord);
    auto mergeMap = mergeTwoMaps(firstMap, secondMap);
    return convertMapToRecord(mergeMap);
}
