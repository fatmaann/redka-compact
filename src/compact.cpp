#include <iostream>
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
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>

#include "merge_records.h"

namespace fs = std::filesystem;

// Конфигурация
const size_t WAL_SIZE_THRESHOLD = 1024; // 1KB
const std::string WAL_FILENAME = "wal.log";
const size_t LEVEL_BASE_SIZE = 10;
const std::string DB_DIR = "lsm_db";

#pragma pack(push, 1)
struct SSTHeader
{
    uint32_t entry_count;
    uint64_t index_offset;
};

struct SSTIndexEntry
{
    uint32_t key_length;
    uint64_t data_offset;
    uint32_t data_length;
};
#pragma pack(pop)

struct FieldValue
{
    uint32_t version;
    std::string value;
};

struct WalEntry
{
    std::string key;
    std::map<std::string, FieldValue> fields;
};

struct SSTEntry
{
    std::string key;
    std::map<std::string, FieldValue> fields;

    bool operator<(const SSTEntry &other) const
    {
        return key < other.key;
    }
};

class MappedFile
{
private:
    int fd = -1;
    void *mapped_data = nullptr;
    size_t file_size = 0;

public:
    MappedFile() = default;

    ~MappedFile()
    {
        if (mapped_data)
        {
            munmap(mapped_data, file_size);
        }
        if (fd != -1)
        {
            close(fd);
        }
    }

    bool open(const std::string &path, bool write = false)
    {
        fd = ::open(path.c_str(), write ? (O_RDWR | O_CREAT) : O_RDONLY, 0644);
        if (fd == -1)
            return false;

        struct stat st;
        if (fstat(fd, &st) == -1)
        { // Исправлено условие
            close(fd);
            fd = -1;
            return false;
        }

        file_size = st.st_size;
        if (file_size == 0 && write)
        {
            file_size = 4096;
            if (ftruncate(fd, file_size) == -1)
            { // Добавлена проверка на ошибку
                close(fd);
                fd = -1;
                return false;
            }
        }

        mapped_data = mmap(nullptr, file_size,
                           write ? (PROT_READ | PROT_WRITE) : PROT_READ,
                           MAP_SHARED, fd, 0);
        if (mapped_data == MAP_FAILED)
        {
            mapped_data = nullptr;
            close(fd);
            fd = -1;
            return false;
        }

        return true;
    }

    void *data() const { return mapped_data; }
    size_t size() const { return file_size; }

    bool resize(size_t new_size)
    {
        if (munmap(mapped_data, file_size))
            return false;
        if (ftruncate(fd, new_size))
            return false;

        mapped_data = mmap(nullptr, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (mapped_data == MAP_FAILED)
        {
            mapped_data = nullptr;
            return false;
        }

        file_size = new_size;
        return true;
    }
};

class LSMTree
{
private:
    std::vector<std::vector<std::string>> levels;

    void ensureDbDir()
    {
        if (!fs::exists(DB_DIR))
        {
            fs::create_directory(DB_DIR);
            for (int i = 0; i < 10; ++i)
            {
                fs::create_directory(DB_DIR + "/L" + std::to_string(i));
            }
        }
    }

    void loadLevels()
    {
        levels.clear();
        for (int i = 0;; ++i)
        {
            std::string level_dir = DB_DIR + "/L" + std::to_string(i);
            if (!fs::exists(level_dir))
                break;

            std::vector<std::string> files;
            for (const auto &entry : fs::directory_iterator(level_dir))
            {
                if (entry.path().extension() == ".sst")
                {
                    files.push_back(entry.path().string());
                }
            }
            std::sort(files.begin(), files.end());
            std::reverse(files.begin(), files.end());
            levels.push_back(files);
        }
    }

    void mergeEntries(SSTEntry& target, const SSTEntry& source) {
        std::string target_record = serializeFields(target.fields);
        std::string source_record = serializeFields(source.fields);
        std::string target_record_with_braces = "{" + target_record + "}";
        std::string source_record_with_braces = "{" + source_record + "}";
        std::string merged_record = mergeTwoRecords(target_record_with_braces, source_record_with_braces);
        target.fields = parseFields(merged_record);
    }

    void compactLevel(int level)
    {
        if (level >= levels.size())
            return;

        std::map<std::string, SSTEntry> merged_entries;

        for (const auto &sst_path : levels[level])
        {
            auto entries = readSST(sst_path);
            for (const auto &entry : entries)
            {
                if (merged_entries.find(entry.key) == merged_entries.end())
                {
                    merged_entries[entry.key] = entry;
                }
                else
                {
                    mergeEntries(merged_entries[entry.key], entry);
                }
            }
        }

        if (merged_entries.size() >= LEVEL_BASE_SIZE * std::pow(10, level))
        {
            std::vector<SSTEntry> entries_to_write;
            for (auto &[key, entry] : merged_entries)
            {
                entries_to_write.push_back(std::move(entry));
            }
            std::sort(entries_to_write.begin(), entries_to_write.end());

            std::string new_sst = DB_DIR + "/L" + std::to_string(level + 1) + "/" +
                                  std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".sst";
            writeSST(new_sst, entries_to_write);

            for (const auto &sst_path : levels[level])
            {
                fs::remove(sst_path);
            }

            loadLevels();
            compactLevel(level + 1);
        }
    }

    std::vector<SSTEntry> readSST(const std::string &path)
    {
        MappedFile file;
        if (!file.open(path))
            return {};

        const char *data = static_cast<const char *>(file.data());
        size_t size = file.size();

        if (size < sizeof(SSTHeader))
            return {};

        SSTHeader header;
        memcpy(&header, data, sizeof(header));

        if (header.entry_count == 0)
            return {};

        size_t index_offset = header.index_offset;
        if (index_offset + header.entry_count * sizeof(SSTIndexEntry) > size)
        {
            return {};
        }

        std::vector<SSTIndexEntry> index(header.entry_count);
        memcpy(index.data(), data + index_offset, header.entry_count * sizeof(SSTIndexEntry));

        std::vector<SSTEntry> entries;
        for (const auto &idx : index)
        {
            if (idx.data_offset + idx.data_length > size)
            {
                continue;
            }

            const char *entry_data = data + idx.data_offset;
            uint32_t total_len;
            memcpy(&total_len, entry_data, sizeof(total_len));

            if (total_len < idx.key_length || idx.key_length == 0)
            {
                continue;
            }

            std::string key(entry_data + sizeof(total_len), idx.key_length);
            std::string fields_data(entry_data + sizeof(total_len) + idx.key_length,
                                    total_len - idx.key_length);

            SSTEntry entry;
            entry.key = key;
            entry.fields = parseFields(fields_data);
            entries.push_back(entry);
        }

        return entries;
    }

    std::map<std::string, FieldValue> parseFields(const std::string &data)
    {
        std::map<std::string, FieldValue> fields;

        std::string content = data;
        if (!content.empty() && content.front() == '{' && content.back() == '}')
        {
            content = content.substr(1, content.size() - 2);
        }

        std::regex field_re(
            R"((\w+)(@(\d+))?:)"
            R"((?:("(?:[^"]|\\")*")|([^ }]+)))");

        std::smatch match;
        std::string::const_iterator search_start(content.cbegin());

        while (std::regex_search(search_start, content.cend(), match, field_re))
        {
            std::string field = match[1].str();
            uint32_t version = match[3].matched ? std::stoul(match[3].str()) : 1;

            std::string value = match[4].matched ? match[4].str() : match[5].str();

            if (value.size() >= 2 && value.front() == '"' && value.back() == '"')
            {
                value = value.substr(1, value.size() - 2);
            }

            fields[field] = {version, value};
            search_start = match[0].second;

            while (search_start != content.cend() && *search_start == ' ')
            {
                ++search_start;
            }
        }

        return fields;
    }

    std::string serializeFields(const std::map<std::string, FieldValue> &fields)
    {
        std::ostringstream oss;
        bool first = true;

        for (const auto &[field, fv] : fields)
        {
            if (!first)
            {
                oss << " ";
            }
            first = false;

            oss << field;
            if (fv.version > 1)
            {
                oss << "@" << fv.version;
            }
            oss << ":" << fv.value;
        }

        return oss.str();
    }

    void writeSST(const std::string &path, const std::vector<SSTEntry> &entries)
    {
        // Сначала вычисляем необходимый размер файла
        size_t total_size = sizeof(SSTHeader);

        // Размер данных
        for (const auto &entry : entries)
        {
            total_size += sizeof(uint32_t); // total_len
            total_size += entry.key.size();
            total_size += serializeFields(entry.fields).size();
        }

        // Размер индекса
        total_size += entries.size() * sizeof(SSTIndexEntry);

        // Создаем и отображаем файл
        MappedFile file;
        if (!file.open(path, true))
        {
            throw std::runtime_error("Failed to create SST file");
        }

        // Увеличиваем файл до нужного размера
        if (file.size() < total_size)
        {
            if (!file.resize(total_size))
            {
                throw std::runtime_error("Failed to resize SST file");
            }
        }

        char *data = static_cast<char *>(file.data());

        // Записываем заголовок
        SSTHeader header;
        header.entry_count = entries.size();
        header.index_offset = sizeof(SSTHeader);
        for (const auto &entry : entries)
        {
            header.index_offset += sizeof(uint32_t) + entry.key.size() + serializeFields(entry.fields).size();
        }
        memcpy(data, &header, sizeof(header));

        // Записываем данные и заполняем индекс
        size_t data_offset = sizeof(SSTHeader);
        std::vector<SSTIndexEntry> index;

        for (const auto &entry : entries)
        {
            std::string fields_data = serializeFields(entry.fields);
            uint32_t total_len = entry.key.size() + fields_data.size();

            // Записываем total_len
            memcpy(data + data_offset, &total_len, sizeof(total_len));
            data_offset += sizeof(total_len);

            // Записываем ключ
            memcpy(data + data_offset, entry.key.data(), entry.key.size());
            data_offset += entry.key.size();

            // Записываем данные полей
            memcpy(data + data_offset, fields_data.data(), fields_data.size());
            data_offset += fields_data.size();

            // Добавляем запись в индекс
            SSTIndexEntry idx;
            idx.key_length = entry.key.size();
            idx.data_offset = data_offset - total_len - sizeof(total_len);
            idx.data_length = total_len;
            index.push_back(idx);
        }

        // Записываем индекс
        memcpy(data + header.index_offset, index.data(), index.size() * sizeof(SSTIndexEntry));
    }

    std::vector<WalEntry> readWalFile(const std::string &filename)
    {
        MappedFile file;
        if (!file.open(filename))
            return {};

        const char *data = static_cast<const char *>(file.data());
        size_t size = file.size();

        std::vector<WalEntry> entries;
        std::string content(data, size);

        std::istringstream iss(content);
        std::string line;

        while (std::getline(iss, line))
        {
            if (line.empty())
                continue;

            size_t key_start = line.find('{');
            size_t key_end = line.find(' ', key_start);
            size_t data_start = line.find('{', key_end);
            size_t data_end = line.rfind('}');

            if (key_start == std::string::npos || key_end == std::string::npos ||
                data_start == std::string::npos || data_end == std::string::npos)
            {
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
    LSMTree()
    {
        ensureDbDir();
        loadLevels();
    }

    void put(const std::string &key, const std::string &value)
    {
        uint64_t timestamp = std::chrono::system_clock::now().time_since_epoch().count();

        // Открываем WAL файл для добавления
        int fd = ::open(WAL_FILENAME.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (fd == -1)
        {
            throw std::runtime_error("Failed to open WAL file");
        }

        std::string entry = "{@" + key + " " + value + "}\n";
        if (write(fd, entry.data(), entry.size()) != static_cast<ssize_t>(entry.size()))
        {
            close(fd);
            throw std::runtime_error("Failed to write to WAL");
        }

        close(fd);

        if (fs::file_size(WAL_FILENAME) >= WAL_SIZE_THRESHOLD)
        {
            flushWalToL0();
        }
    }

    void flushWalToL0()
    {
        auto wal_entries = readWalFile(WAL_FILENAME);
        std::map<std::string, SSTEntry> latest_entries;

        for (const auto &wal_entry : wal_entries)
        {
            if (latest_entries.find(wal_entry.key) == latest_entries.end())
            {
                SSTEntry entry;
                entry.key = wal_entry.key;
                latest_entries[wal_entry.key] = entry;
            }
            for (const auto &[field, fv] : wal_entry.fields)
            {
                latest_entries[wal_entry.key].fields[field] = fv;
            }
        }

        std::vector<SSTEntry> entries;
        for (auto &[key, entry] : latest_entries)
        {
            entries.push_back(std::move(entry));
        }

        std::string sst_path = DB_DIR + "/L0/" +
                               std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".sst";
        writeSST(sst_path, entries);

        // Очищаем WAL файл
        int fd = ::open(WAL_FILENAME.c_str(), O_WRONLY | O_TRUNC, 0644);
        if (fd != -1)
        {
            close(fd);
        }

        loadLevels();
        compactLevel(0);
    }

    std::string get(const std::string &key)
    {
        loadLevels();
        std::map<std::string, FieldValue> merged_fields;

        // Проверяем WAL
        auto wal_entries = readWalFile(WAL_FILENAME);
        for (const auto &entry : wal_entries)
        {
            if (entry.key == key)
            {
                for (const auto &[field, fv] : entry.fields)
                {
                    if (fv.version > merged_fields[field].version)
                    {
                        merged_fields[field] = fv;
                    }
                }
            }
        }

        // Ищем в SST-файлах
        for (const auto &level : levels)
        {
            for (const auto &sst_path : level)
            {
                auto entries = readSST(sst_path);
                auto it = std::lower_bound(
                    entries.begin(), entries.end(), key,
                    [](const SSTEntry &e, const std::string &k)
                    { return e.key < k; });

                if (it != entries.end() && it->key == key)
                {
                    for (const auto &[field, fv] : it->fields)
                    {
                        if (fv.version > merged_fields[field].version)
                        {
                            merged_fields[field] = fv;
                        }
                    }
                }
            }
        }

        return merged_fields.empty() ? "" : serializeFields(merged_fields);
    }
};

// int main()
// {
//     LSMTree db;
//     auto check = 2;
//     if (check == 1){
//         db.put("b0b-1", "{name:\"Alena\" age:25}");
//         db.put("b0b-1", "{name:\"Alena\" age:26 address:\"Moscow\"}");
//         db.put("b0b-1", "{address@2:\"SPb\"}");
//     } else {
//         db.put("b0b-1", "{name:\"Alena\" age:25}");
//         db.put("b0b-1", "{name:\"Alena\" age:26 address:\"Moscow\"}");
//         db.put("b0b-1", "{address@2:\"KZN\"}");

//         for (size_t i = 0; i < 1; i++) {
//             db.put("b0b-5", "{name:\"Dima\"}");
//             db.put("b0b-6", "{name:\"Dima\"}");
//             db.put("b0b-7", "{name:\"Dima\"}");
//             db.put("b0b-8", "{name:\"Dima\"}");
//             db.put("b0b-9", "{name:\"Dima\"}");
//             db.put("b0b-10", "{name:\"Dima\"}");
//             db.put("b0b-11", "{name:\"Dima\"}");
//             db.put("b0b-12", "{name:\"Dima\"}");
//             db.put("b0b-13", "{name:\"Dima\"}");
//         }
//     }
    
//     db.flushWalToL0();

//     std::cout << "b0b-1: " << db.get("b0b-1") << std::endl;
//     std::cout << "b0b-10: " << db.get("b0b-10") << std::endl;

//     return 0;
// }
