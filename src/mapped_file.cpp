#include "mapped_file.h"

#include <cstring>
#include <cstdio>

size_t WAL_LOG_MAX_SIZE = 4096;

MappedFile::MappedFile(const std::string &file_name) {
    if (!this->open(file_name, true)) {
        throw std::system_error(errno, std::system_category(), "File mapping failed");
    }
}

// Реализация методов MappedFile
MappedFile::~MappedFile() {
    if (mapped_data_) {
        munmap(mapped_data_, file_size_);
    }
    if (fd_ != -1) {
        close(fd_);
    }
}

bool MappedFile::open(const std::string &path, bool write) {
    fd_ = ::open(path.c_str(), write ? (O_RDWR | O_CREAT) : O_RDONLY, 0644);
    if (fd_ == -1)
        return false;

    struct stat st;
    if (fstat(fd_, &st) == -1) {
        close(fd_);
        fd_ = -1;
        return false;
    }

    file_size_ = st.st_size;
    if (file_size_ == 0 && write) {
        records_size_ = 0;
        file_size_ = WAL_LOG_MAX_SIZE;
        if (ftruncate(fd_, file_size_) == -1) {
            close(fd_);
            fd_ = -1;
            return false;
        }
    }
    else {
        records_size_ = file_size_;
    }

    mapped_data_ = static_cast<char *>(
        mmap(nullptr, file_size_, write ? (PROT_READ | PROT_WRITE) : PROT_READ, MAP_SHARED, fd_, 0));
    if (mapped_data_ == MAP_FAILED) {
        mapped_data_ = nullptr;
        close(fd_);
        fd_ = -1;
        return false;
    }

    return true;
}

char *MappedFile::data() const {
    return mapped_data_;
}
size_t MappedFile::size() const {
    return records_size_;
}

bool MappedFile::resize(size_t new_size) {
    if (munmap(mapped_data_, file_size_))
        return false;
    if (ftruncate(fd_, new_size))
        return false;

    mapped_data_ = static_cast<char *>(mmap(nullptr, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
    if (mapped_data_ == MAP_FAILED) {
        mapped_data_ = nullptr;
        return false;
    }

    file_size_ = new_size;
    return true;
}

void MappedFile::append(const std::string &logEntry) {
    if (records_size_ >= file_size_) {
        perror("WAL log is too big");
        return;
    }

    // Append the log entry at the old file end
    memcpy(mapped_data_ + records_size_, logEntry.data(), logEntry.size());
    records_size_ += logEntry.size();

    // Flush changes to disk
    if (msync(mapped_data_, file_size_, MS_SYNC) == -1) {
        perror("msync");
    }
}

void MappedFile::truncate() {
    if (fd_ == -1) return;

    // 1. Удаляем текущее отображение памяти
    if (mapped_data_) {
        munmap(mapped_data_, file_size_);
        mapped_data_ = nullptr;
    }

    // 2. Усекаем файл до нулевого размера
    if (ftruncate(fd_, 0) == -1) {
        throw std::system_error(errno, std::system_category(), "ftruncate failed");
    }

    // 3. Устанавливаем начальный размер (например, 4KB)
    file_size_ = 4096;
    if (ftruncate(fd_, file_size_) == -1) {
        close(fd_);
        fd_ = -1;
        throw std::system_error(errno, std::system_category(), "Initial truncate failed");
    }

    // 4. Создаем новое отображение
    mapped_data_ = static_cast<char*>(
        mmap(nullptr, file_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
    if (mapped_data_ == MAP_FAILED) {
        mapped_data_ = nullptr;
        close(fd_);
        fd_ = -1;
        throw std::system_error(errno, std::system_category(), "mmap failed after truncate");
    }
}