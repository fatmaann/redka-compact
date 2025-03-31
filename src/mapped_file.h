#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>
#include <system_error>

// Класс для работы с memory-mapped файлами
class MappedFile {
private:
    int fd_ = -1;
    char *mapped_data_ = nullptr;
    size_t file_size_ = 0;

public:
    MappedFile() = default;
    MappedFile(const std::string &);
    ~MappedFile();

    bool open(const std::string &path, bool write = false);
    char *data() const;
    size_t size() const;
    bool resize(size_t new_size);
    void append(const std::string &);
    void truncate();
};