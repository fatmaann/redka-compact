#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <filesystem>
#include <chrono>
#include <thread>
#include <algorithm>
#include <cmath>
#include <sstream>
#include <regex>

namespace fs = std::filesystem;

// Конфигурация
const size_t WAL_SIZE_THRESHOLD = 1024; // 1KB
const std::string WAL_FILENAME = "wal.log";
const size_t LEVEL_BASE_SIZE = 10;
const std::string DB_DIR = "lsm_db";

#pragma pack(push, 1)
struct SSTHeader {
    uint32_t version = 2; // Версия 2 для поддержки версионирования полей
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

struct WalEntry {
    std::string key;
    std::map<std::string, FieldValue> fields;
};

struct SSTEntry {
    std::string key;
    std::map<std::string, FieldValue> fields;

    bool operator<(const SSTEntry& other) const {
        return key < other.key;
    }
};

class LSMTree {
private:
    std::vector<std::vector<std::string>> levels;

    void ensureDbDir() {
        if (!fs::exists(DB_DIR)) {
            fs::create_directory(DB_DIR);
            for (int i = 0; i < 10; ++i) {
                fs::create_directory(DB_DIR + "/L" + std::to_string(i));
            }
        }
    }

    void loadLevels() {
        levels.clear();
        for (int i = 0; ; ++i) {
            std::string level_dir = DB_DIR + "/L" + std::to_string(i);
            if (!fs::exists(level_dir)) break;

            std::vector<std::string> files;
            for (const auto& entry : fs::directory_iterator(level_dir)) {
                if (entry.path().extension() == ".sst") {
                    files.push_back(entry.path().string());
                }
            }
            std::sort(files.begin(), files.end());
            std::reverse(files.begin(), files.end());
            levels.push_back(files);
        }
    }

    void mergeEntries(SSTEntry& target, const SSTEntry& source) {
        for (const auto& [field, src_val] : source.fields) {
            auto it = target.fields.find(field);
            if (it == target.fields.end() || src_val.version > it->second.version) {
                target.fields[field] = src_val;
            }
        }
    }

    void compactLevel(int level) {
        if (level >= levels.size()) return;
        
        std::map<std::string, SSTEntry> merged_entries;
        
        // Собираем и мержим все записи с текущего уровня
        for (const auto& sst_path : levels[level]) {
            auto entries = readSST(sst_path);
            for (const auto& entry : entries) {
                if (merged_entries.find(entry.key) == merged_entries.end()) {
                    merged_entries[entry.key] = entry;
                } else {
                    mergeEntries(merged_entries[entry.key], entry);
                }
            }
        }
    
        if (merged_entries.size() >= LEVEL_BASE_SIZE * std::pow(10, level)) {
            // Преобразуем map в vector для записи
            std::vector<SSTEntry> entries_to_write;
            for (auto& [key, entry] : merged_entries) {
                entries_to_write.push_back(std::move(entry));
            }
            std::sort(entries_to_write.begin(), entries_to_write.end());
    
            // Записываем на следующий уровень
            std::string new_sst = DB_DIR + "/L" + std::to_string(level + 1) + "/" +
                                 std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".sst";
            writeSST(new_sst, entries_to_write);
    
            // Удаляем старые файлы
            for (const auto& sst_path : levels[level]) {
                fs::remove(sst_path);
            }
    
            // Обновляем индексы и рекурсивно уплотняем
            loadLevels();
            compactLevel(level + 1);
        }
    }

    std::vector<SSTEntry> readSST(const std::string& path) {
        std::ifstream file(path, std::ios::binary);
        if (!file) return {};

        SSTHeader header;
        file.read(reinterpret_cast<char*>(&header), sizeof(header));

        std::vector<SSTIndexEntry> index(header.entry_count);
        file.seekg(header.index_offset);
        file.read(reinterpret_cast<char*>(index.data()), header.entry_count * sizeof(SSTIndexEntry));

        std::vector<SSTEntry> entries;
        for (const auto& idx : index) {
            file.seekg(idx.data_offset);
            
            uint32_t total_len;
            file.read(reinterpret_cast<char*>(&total_len), sizeof(total_len));

            std::string key(idx.key_length, '\0');
            file.read(&key[0], idx.key_length);

            std::string fields_data(total_len - idx.key_length, '\0');
            file.read(&fields_data[0], fields_data.size());

            SSTEntry entry;
            entry.key = key;
            entry.fields = parseFields(fields_data);
            entries.push_back(entry);
        }

        return entries;
    }

    std::map<std::string, FieldValue> parseFields(const std::string& data) {
        std::map<std::string, FieldValue> fields;
        std::regex field_re(R"((\w+)(@(\d+))?:([^ ]+))");
        std::smatch match;
        std::string::const_iterator search_start(data.cbegin());

        while (std::regex_search(search_start, data.cend(), match, field_re)) {
            std::string field = match[1].str();
            uint32_t version = match[3].matched ? std::stoul(match[3].str()) : 1;
            std::string value = match[4].str();

            fields[field] = {version, value};
            search_start = match[0].second;
        }

        return fields;
    }

    std::string serializeFields(const std::map<std::string, FieldValue>& fields) {
        std::stringstream ss;
        ss << "{";
        bool first = true;
        for (const auto& [field, fv] : fields) {
            if (!first) ss << " ";
            first = false;
            if (fv.version > 1) {
                ss << field << "@" << fv.version << ":" << fv.value;
            } else {
                ss << field << ":" << fv.value;
            }
        }
        ss << "}";
        return ss.str();
    }

    void writeSST(const std::string& path, const std::vector<SSTEntry>& entries) {
        std::ofstream file(path, std::ios::binary);
        SSTHeader header;
        header.entry_count = entries.size();
        file.write(reinterpret_cast<char*>(&header), sizeof(header));

        std::vector<SSTIndexEntry> index;
        for (const auto& entry : entries) {
            SSTIndexEntry idx;
            idx.key_length = entry.key.size();
            idx.data_offset = file.tellp();

            std::string fields_data = serializeFields(entry.fields);
            uint32_t total_len = entry.key.size() + fields_data.size();
            
            file.write(reinterpret_cast<char*>(&total_len), sizeof(total_len));
            file.write(entry.key.data(), entry.key.size());
            file.write(fields_data.data(), fields_data.size());

            idx.data_length = total_len;
            index.push_back(idx);
        }

        header.index_offset = file.tellp();
        for (const auto& idx : index) {
            file.write(reinterpret_cast<const char*>(&idx), sizeof(idx));
        }

        file.seekp(0);
        file.write(reinterpret_cast<char*>(&header), sizeof(header));
    }

    std::vector<WalEntry> readWalFile(const std::string& filename) {
        std::vector<WalEntry> entries;
        std::ifstream wal(filename);
        if (!wal) return entries;
    
        std::string line;
        while (std::getline(wal, line)) {
            if (line.empty()) continue;
    
            size_t key_start = line.find('{');
            size_t key_end = line.find(' ', key_start);
            size_t data_start = line.find('{', key_end);
            size_t data_end = line.rfind('}');
    
            if (key_start == std::string::npos || key_end == std::string::npos || 
                data_start == std::string::npos || data_end == std::string::npos) {
                std::cerr << "Invalid WAL entry: " << line << std::endl;
                continue;
            }
    
            std::string key = line.substr(key_start + 2, key_end - (key_start + 2));
            std::string fields_data = line.substr(data_start, data_end - data_start + 1);
    
            WalEntry entry;
            entry.key = key;
            entry.fields = parseFields(fields_data);
            entries.push_back(entry);
        }
    
        return entries;
    }

public:
    LSMTree() {
        ensureDbDir();
        loadLevels();
    }

    void put(const std::string& key, const std::string& value) {
        uint64_t timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        std::ofstream wal(WAL_FILENAME, std::ios::app);
        wal << "{@" << key << " " << value << "}\n";
        wal.close();
    
        if (fs::file_size(WAL_FILENAME) >= WAL_SIZE_THRESHOLD) {
            flushWalToL0();
        }
    }

    void flushWalToL0() {
        auto wal_entries = readWalFile(WAL_FILENAME);
        std::map<std::string, SSTEntry> latest_entries;
        
        for (const auto& wal_entry : wal_entries) {
            if (latest_entries.find(wal_entry.key) == latest_entries.end()) {
                SSTEntry entry;
                entry.key = wal_entry.key;
                latest_entries[wal_entry.key] = entry;
            }
            for (const auto& [field, fv] : wal_entry.fields) {
                latest_entries[wal_entry.key].fields[field] = fv;
            }
        }
    
        std::vector<SSTEntry> entries;
        for (auto& [key, entry] : latest_entries) {
            entries.push_back(std::move(entry));
        }
    
        std::string sst_path = DB_DIR + "/L0/" +
                              std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".sst";
        writeSST(sst_path, entries);
    
        std::ofstream wal(WAL_FILENAME, std::ios::trunc);
        loadLevels();
        compactLevel(0);
    }

    std::string get(const std::string& key) {
        std::map<std::string, FieldValue> merged_fields;
    
        // Проверяем WAL
        auto wal_entries = readWalFile(WAL_FILENAME);
        for (const auto& entry : wal_entries) {
            if (entry.key == key) {
                for (const auto& [field, fv] : entry.fields) {
                    if (fv.version > merged_fields[field].version) {
                        merged_fields[field] = fv;
                    }
                }
            }
        }
    
        // Ищем в SST-файлах
        for (const auto& level : levels) {
            for (const auto& sst_path : level) {
                auto entries = readSST(sst_path);
                auto it = std::lower_bound(
                    entries.begin(), entries.end(), key,
                    [](const SSTEntry& e, const std::string& k) { return e.key < k; });
    
                if (it != entries.end() && it->key == key) {
                    for (const auto& [field, fv] : it->fields) {
                        if (fv.version > merged_fields[field].version) {
                            merged_fields[field] = fv;
                        }
                    }
                }
            }
        }
        
        return merged_fields.empty() ? "" : serializeFields(merged_fields);
    }
};
