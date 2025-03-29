#include "compact.h"

// Реализация методов MappedFile
MappedFile::~MappedFile() {
    if (mapped_data) {
        munmap(mapped_data, file_size);
    }
    if (fd != -1) {
        close(fd);
    }
}

bool MappedFile::open(const std::string &path, bool write) {
    fd = ::open(path.c_str(), write ? (O_RDWR | O_CREAT) : O_RDONLY, 0644);
    if (fd == -1) return false;

    struct stat st;
    if (fstat(fd, &st) == -1) {
        close(fd);
        fd = -1;
        return false;
    }

    file_size = st.st_size;
    if (file_size == 0 && write) {
        file_size = 4096;
        if (ftruncate(fd, file_size) == -1) {
            close(fd);
            fd = -1;
            return false;
        }
    }

    mapped_data = mmap(nullptr, file_size,
                     write ? (PROT_READ | PROT_WRITE) : PROT_READ,
                     MAP_SHARED, fd, 0);
    if (mapped_data == MAP_FAILED) {
        mapped_data = nullptr;
        close(fd);
        fd = -1;
        return false;
    }

    return true;
}

void *MappedFile::data() const { return mapped_data; }
size_t MappedFile::size() const { return file_size; }

bool MappedFile::resize(size_t new_size) {
    if (munmap(mapped_data, file_size)) return false;
    if (ftruncate(fd, new_size)) return false;

    mapped_data = mmap(nullptr, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped_data == MAP_FAILED) {
        mapped_data = nullptr;
        return false;
    }

    file_size = new_size;
    return true;
}

// Реализация методов LSMTree
LSMTree::LSMTree() {
    ensureDbDir();
    loadLevels();
}

void LSMTree::ensureDbDir() {
    if (!fs::exists(DB_DIR)) {
        fs::create_directory(DB_DIR);
        for (int i = 0; i < 10; ++i) {
            fs::create_directory(DB_DIR + "/L" + std::to_string(i));
        }
    }
}

void LSMTree::loadLevels() {
    levels.clear();
    for (int i = 0;; ++i) {
        std::string level_dir = DB_DIR + "/L" + std::to_string(i);
        if (!fs::exists(level_dir)) break;

        std::vector<std::string> files;
        for (const auto &entry : fs::directory_iterator(level_dir)) {
            if (entry.path().extension() == ".sst") {
                files.push_back(entry.path().string());
            }
        }
        std::sort(files.begin(), files.end());
        std::reverse(files.begin(), files.end());
        levels.push_back(files);
    }
}

void LSMTree::mergeEntries(SSTEntry& target, const SSTEntry& source) {
    std::string target_record = serializeFields(target.fields);
    std::string source_record = serializeFields(source.fields);
    std::string target_record_with_braces = "{" + target_record + "}";
    std::string source_record_with_braces = "{" + source_record + "}";

    std::cout << target_record_with_braces << ' ' << source_record_with_braces << '\n';

    std::string merged_record = mergeTwoRecords(target_record_with_braces, source_record_with_braces);
    std::cout << merged_record << "\n\n";
    target.fields = parseFields(merged_record);
}

void LSMTree::compactLevel(int level) {
    if (level >= levels.size()) return;

    std::map<std::string, SSTEntry> merged_entries;

    for (const auto &sst_path : levels[level]) {
        auto entries = readSST(sst_path);
        for (const auto &entry : entries) {
            if (merged_entries.find(entry.key) == merged_entries.end()) {
                merged_entries[entry.key] = entry;
            } else {
                mergeEntries(merged_entries[entry.key], entry);
            }
        }
    }

    if (merged_entries.size() >= LEVEL_BASE_SIZE * std::pow(10, level)) {
        std::vector<SSTEntry> entries_to_write;
        for (auto &[key, entry] : merged_entries) {
            entries_to_write.push_back(std::move(entry));
        }
        std::sort(entries_to_write.begin(), entries_to_write.end());

        std::string new_sst = DB_DIR + "/L" + std::to_string(level + 1) + "/" +
                            std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".sst";
        writeSST(new_sst, entries_to_write);

        for (const auto &sst_path : levels[level]) {
            fs::remove(sst_path);
        }

        loadLevels();
        compactLevel(level + 1);
    }
}

std::vector<SSTEntry> LSMTree::readSST(const std::string &path) {
    MappedFile file;
    if (!file.open(path)) return {};

    const char *data = static_cast<const char *>(file.data());
    size_t size = file.size();

    if (size < sizeof(SSTHeader)) return {};

    SSTHeader header;
    memcpy(&header, data, sizeof(header));

    if (header.entry_count == 0) return {};

    size_t index_offset = header.index_offset;
    if (index_offset + header.entry_count * sizeof(SSTIndexEntry) > size) {
        return {};
    }

    std::vector<SSTIndexEntry> index(header.entry_count);
    memcpy(index.data(), data + index_offset, header.entry_count * sizeof(SSTIndexEntry));

    std::vector<SSTEntry> entries;
    for (const auto &idx : index) {
        if (idx.data_offset + idx.data_length > size) {
            continue;
        }

        const char *entry_data = data + idx.data_offset;
        uint32_t total_len;
        memcpy(&total_len, entry_data, sizeof(total_len));

        if (total_len < idx.key_length || idx.key_length == 0) {
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

std::map<std::string, FieldValue> LSMTree::parseFields(const std::string &data) {
    std::map<std::string, FieldValue> fields;

    std::string content = data;
    if (!content.empty() && content.front() == '{' && content.back() == '}') {
        content = content.substr(1, content.size() - 2);
    }

    std::regex field_re(
        R"((\w+)(@(\d+))?:)"
        R"((?:("(?:[^"]|\\")*")|([^ }]+)))");

    std::smatch match;
    std::string::const_iterator search_start(content.cbegin());

    while (std::regex_search(search_start, content.cend(), match, field_re)) {
        std::string field = match[1].str();
        uint32_t version = match[3].matched ? std::stoul(match[3].str()) : 1;

        std::string value = match[4].matched ? match[4].str() : match[5].str();

        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2);
        }

        fields[field] = {version, value};
        search_start = match[0].second;

        while (search_start != content.cend() && *search_start == ' ') {
            ++search_start;
        }
    }

    return fields;
}

std::string LSMTree::serializeFields(const std::map<std::string, FieldValue> &fields) {
    std::ostringstream oss;
    bool first = true;

    for (const auto &[field, fv] : fields) {
        if (!first) {
            oss << " ";
        }
        first = false;

        oss << field;
        if (fv.version > 1) {
            oss << "@" << fv.version;
        }
        oss << ":" << fv.value;
    }

    return oss.str();
}

void LSMTree::writeSST(const std::string &path, const std::vector<SSTEntry> &entries) {
    size_t total_size = sizeof(SSTHeader);

    for (const auto &entry : entries) {
        total_size += sizeof(uint32_t);
        total_size += entry.key.size();
        total_size += serializeFields(entry.fields).size();
    }

    total_size += entries.size() * sizeof(SSTIndexEntry);

    MappedFile file;
    if (!file.open(path, true)) {
        throw std::runtime_error("Failed to create SST file");
    }

    if (file.size() < total_size) {
        if (!file.resize(total_size)) {
            throw std::runtime_error("Failed to resize SST file");
        }
    }

    char *data = static_cast<char *>(file.data());

    SSTHeader header;
    header.entry_count = entries.size();
    header.index_offset = sizeof(SSTHeader);
    for (const auto &entry : entries) {
        header.index_offset += sizeof(uint32_t) + entry.key.size() + serializeFields(entry.fields).size();
    }
    memcpy(data, &header, sizeof(header));

    size_t data_offset = sizeof(SSTHeader);
    std::vector<SSTIndexEntry> index;

    for (const auto &entry : entries) {
        std::string fields_data = serializeFields(entry.fields);
        uint32_t total_len = entry.key.size() + fields_data.size();

        memcpy(data + data_offset, &total_len, sizeof(total_len));
        data_offset += sizeof(total_len);

        memcpy(data + data_offset, entry.key.data(), entry.key.size());
        data_offset += entry.key.size();

        memcpy(data + data_offset, fields_data.data(), fields_data.size());
        data_offset += fields_data.size();

        SSTIndexEntry idx;
        idx.key_length = entry.key.size();
        idx.data_offset = data_offset - total_len - sizeof(total_len);
        idx.data_length = total_len;
        index.push_back(idx);
    }

    memcpy(data + header.index_offset, index.data(), index.size() * sizeof(SSTIndexEntry));
}

std::string LSMTree::mergeTwoRecords(const std::string& firstRecord, const std::string& secondRecord) {
    auto firstMap = parseFields(firstRecord);
    auto secondMap = parseFields(secondRecord);
    
    for (const auto& [field, fv] : secondMap) {
        auto it = firstMap.find(field);
        if (it == firstMap.end() || fv.version > it->second.version) {
            firstMap[field] = fv;
        }
    }
    
    return serializeFields(firstMap);
}

void LSMTree::put(const std::string &key, const std::string &value) {
    SSTEntry entry;
    entry.key = key;
    entry.fields = parseFields(value);
    
    std::vector<SSTEntry> entries;
    entries.push_back(entry);
    
    std::string sst_path = DB_DIR + "/L0/" +
        std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".sst";
    writeSST(sst_path, entries);
    
    loadLevels();
    compactLevel(0);
}

void LSMTree::flushBatchToL0(const std::vector<std::pair<std::string, std::string>>& batch) {
    std::map<std::string, SSTEntry> latest_entries;

    for (const auto& [key, value] : batch) {
        SSTEntry new_entry;
        new_entry.key = key;
        new_entry.fields = parseFields(value);

        auto it = latest_entries.find(key);
        if (it == latest_entries.end()) {
            latest_entries[key] = new_entry;
        } else {
            mergeEntries(new_entry, it->second);
            it->second = new_entry;
        }
    }

    std::vector<SSTEntry> entries;
    for (auto& [key, entry] : latest_entries) {
        entries.push_back(std::move(entry));
    }
    std::string sst_path = DB_DIR + "/L0/" +
        std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".sst";
    writeSST(sst_path, entries);
    loadLevels();
    compactLevel(0);
}

std::string LSMTree::get(const std::string &key) {
    loadLevels();
    std::map<std::string, FieldValue> merged_fields;

    for (const auto &level : levels) {
        for (const auto &sst_path : level) {
            auto entries = readSST(sst_path);
            auto it = std::lower_bound(
                entries.begin(), entries.end(), key,
                [](const SSTEntry &e, const std::string &k) { return e.key < k; });

            if (it != entries.end() && it->key == key) {
                for (const auto &[field, fv] : it->fields) {
                    if (fv.version > merged_fields[field].version) {
                        merged_fields[field] = fv;
                    }
                }
            }
        }
    }

    return merged_fields.empty() ? "" : serializeFields(merged_fields);
}