#ifndef LSM_TREE_H
#define LSM_TREE_H

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <map>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// Конфигурационные константы
const size_t LEVEL_BASE_SIZE = 10;
const std::string DB_DIR = "lsm_db";

// Структуры для хранения данных
#pragma pack(push, 1)
struct SSTHeader {
    uint32_t entry_count;
    uint64_t index_offset;
};

struct SSTIndexEntry {
    uint32_t key_length;
    uint64_t data_offset;
    uint32_t data_length;
};
#pragma pack(pop)

struct FieldValue {
    uint32_t version;
    std::string value;
};

struct SSTEntry {
    std::string key;
    std::map<std::string, FieldValue> fields;

    bool operator<(const SSTEntry &other) const {
        return key < other.key;
    }
};

// Основной класс LSM-дерева
class LSMTree {
private:
    std::vector<std::vector<std::string>> levels;

    // Внутренние методы
    void ensureDbDir();
    void loadLevels();
    void mergeEntries(SSTEntry &target, const SSTEntry &source);
    void compactLevel(int level);
    std::vector<SSTEntry> readSST(const std::string &path);
    std::map<std::string, FieldValue> parseFields(const std::string &data);
    std::string serializeFields(const std::map<std::string, FieldValue> &fields);
    void writeSST(const std::string &path, const std::vector<SSTEntry> &entries);
    std::string mergeTwoRecords(const std::string &firstRecord, const std::string &secondRecord);

public:
    LSMTree();
    void put(const std::string &key, const std::string &value);
    void flushBatchToL0(const std::vector<std::pair<std::string, std::string>> &batch);
    std::string get(const std::string &key);
};

#endif  // LSM_TREE_H