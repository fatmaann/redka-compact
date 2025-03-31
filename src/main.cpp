#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cstddef>
#include <cstring>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>

#include "coro_task.h"
#include "executor.h"
#include "compact.h"
#include "mapped_file.h"
#include "merge_records.h"
#include "net.h"
#include "uuid_v4.h"

using redka::io::Acceptor;
using redka::io::CoroResult;
using redka::io::Executor;
using redka::io::TcpSocket;

const std::string WAL_FILENAME = "wal.log";
const size_t MAX_WAL_SIZE = 4ULL * 1024 * 1024 * 1024; // 4 GB
bool wal_size_exceeded = false;
LSMTree db;

// Hash table: u128 -> std::tuple[size_t, size_t][4] (for records offsets and lengths for faster reading)
std::unordered_map<std::string, std::array<std::pair<size_t, size_t>, 4>> recordIdToOffset{};
UUIDv4::UUIDGenerator<std::mt19937_64> uuidGenerator;

// Response codes, starting from 1: errors
const int RDKAnone = 0;
const int RDKAbad = 1;
const int RDXbad = 2;

MappedFile wal_log = MappedFile(WAL_FILENAME);

std::string readFromWALFileByOffset(MappedFile &mmapFile, const size_t recordOffset, const size_t recordLength) {
    if (recordOffset >= mmapFile.size()) {
        // Handle error: offset beyond file size.
        return "";
    }
    char *recordStart = mmapFile.data() + recordOffset;
    return std::string(recordStart, recordLength);
}

std::string readFromWALFileById(const std::string &recordId) {
    std::string mergedRecord;
    auto recordsMetadata = recordIdToOffset[recordId];
    for (auto &recordMetadata : recordsMetadata) {
        if (recordMetadata.first == -1)
            break;

        auto previousLogEntry = readFromWALFileByOffset(wal_log, recordMetadata.first, recordMetadata.second);
        mergedRecord = mergeTwoRecords(mergedRecord, previousLogEntry);
    }
    return mergedRecord;
}

void appendToWAL(MappedFile &mmapFile, const std::string &logEntry) {
    mmapFile.append(logEntry + '\n');
}

// Function to write WAL to a log file
void writeWALToFile(const std::string &logEntry, std::string const &recordId) {
    if (wal_log.size() > MAX_WAL_SIZE) {
        std::vector<std::pair<std::string, std::string>> batch;
        for (const auto& [id, offsets] : recordIdToOffset) {
            std::string record = readFromWALFileById(id);
            if (!record.empty()) {
                batch.emplace_back(id, record);
            }
        }
        
        if (!batch.empty()) {
            db.flushBatchToL0(batch);
        }
        
        wal_log.truncate();
        recordIdToOffset.clear();
    }

    size_t newRecordOffset = wal_log.size();
    if (recordIdToOffset.find(recordId) == recordIdToOffset.end()) {
        appendToWAL(wal_log, logEntry);
        recordIdToOffset[recordId] = {std::make_pair(newRecordOffset, logEntry.size()),
                                      {-1u, 0}, {-1u, 0}, {-1u, 0}};
    } else {
        auto &recordsOffsets = recordIdToOffset[recordId];
        bool fourWritesAreTracked = true;
        for (auto &recordMetadata : recordsOffsets) {
            // no offset
            if (recordMetadata.first == -1) {
                recordMetadata.first = newRecordOffset;
                recordMetadata.second = logEntry.size();
                fourWritesAreTracked = false;
                appendToWAL(wal_log, logEntry);
                break;
            }
        }
        if (fourWritesAreTracked) {
            // Merge all four writes and add it
            std::string mergedRecord = logEntry;
            mergedRecord = mergeTwoRecords(mergedRecord, readFromWALFileById(recordId));
            std::stringstream new_record;
            new_record << "{@" << recordId << " " << mergedRecord << "}";
            recordIdToOffset[recordId] = {std::make_pair(newRecordOffset, new_record.str().size()),
                                          {-1u, 0}, {-1u, 0}, {-1u, 0}};
            appendToWAL(wal_log, new_record.str());
        }
    }
}

bool isCorrectParentheses(char firstSymbol, char secondSymbol) {
    if ((firstSymbol == '{' && secondSymbol != '}') || (firstSymbol != '{' && secondSymbol == '}'))
        return false;
    if ((firstSymbol == '<' && secondSymbol != '>') || (firstSymbol != '<' && secondSymbol == '>'))
        return false;
    return true;
}

// Parse an JDR message in format "{...}"
// NB: Records must be in format
//    {@1 {address@2:"Wonderland"}}    | {@1 {address:"Home" name:"Alice"}}
// or
//    {@1 {<@2 address,"Wonderland">}} | {@1 {<address,"Home"> <name, "Alice">}}
// or even (mixed case)
//    {@1 {<address,"Home"> name:"Alice"}}
// For merge to work correctly (so value always must be in {} brackets)
bool parseWriteMessage(const std::string &message, std::string &objectData, bool &isUpdate, std::string &updateIndex) {
    // As record ID is not a version, we are introducing special format for
    // queries including it We demand that message starts and ends with "{" and
    // "}" brackets
    if (message.length() <= 2 || *message.begin() != '{' || *(message.end() - 1) != '}')
        return false;
    // Records are separated by "\n", so it is definitely forbidden (and also by format itself)
    if (message.find('\n') != std::string::npos)
        return false;

    // First symbol always {, so in case of update second symbol must be @ marking
    // versioning by id
    isUpdate = (message.find('@') == 1);

    std::string record;
    // ID ends with space and follows by record in one of the following format
    // (PLEX object): {...} | <...> | ...
    if (isUpdate) {
        size_t spacePos = message.find(' ');
        // Validating record format
        if (!isCorrectParentheses(message[spacePos + 1], message[message.size() - 2]))
            return false;

        updateIndex = message.substr(2, spacePos - 2);
        if (message[spacePos + 1] != '{') {
            record = "{" + message.substr(spacePos + 1, message.size() - spacePos - 2) + "}";
        } else {
            record = message.substr(spacePos + 1, message.size() - spacePos - 2);
        }
        objectData = message.substr(0, spacePos + 1) + record + "}";
        return true;
    }

    // New object writes must be either {...} or <...>
    if (!isCorrectParentheses(*message.begin(), *(message.end() - 1)))
        return false;
    objectData = message;
    return true;
}

bool parseMessage(const std::string &message, std::string &objectData, bool &isRead, bool &isUpdate,
                  std::string &updateIndex) {
    if (message.find('{') == std::string::npos && message.find('}') == std::string::npos && *message.begin() != '@') {
        // Got read query: only reference of object
        isRead = true;
        objectData = message;
        return true;
    }
    return parseWriteMessage(message, objectData, isUpdate, updateIndex);
}

// Handle the client connection
CoroResult<void> handleClient(TcpSocket socket) {
    std::array<char, 1024> buffer;

    while (true) {
        size_t bytesRead = co_await socket.ReadSome(buffer);
        if (bytesRead <= 0) {
            co_await socket.WriteAll(std::span(std::to_string(RDKAbad).c_str(), 1));
            break;
        }

        std::string message(buffer.begin(), buffer.begin() + bytesRead);
        std::string idOrRecord;
        bool isRead = false;
        bool isUpdate = false;
        std::string idOfRecordToUpdate;

        bool gorParseError = false;
        try {
            if (!parseMessage(message, idOrRecord, isRead, isUpdate, idOfRecordToUpdate)) {
                co_await socket.WriteAll(std::span(std::to_string(RDKAbad).c_str(), 1));
                break;
            }
        } catch (...) {
            gorParseError = true;
            // 'co_await' cannot be used in the handler of a try block
        }
        if (gorParseError) {
            co_await socket.WriteAll(std::span(std::to_string(RDXbad).c_str(), 1));
            break;
        }

        // Read query
        if (isRead) {
            // Check if is correct UUID by trying to parse it
            bool gotUnclearID = false;
            try {
                UUIDv4::UUID::fromStrFactory(idOrRecord);
            } catch (...) {
                gotUnclearID = true;
            }
            if (gotUnclearID) {
                co_await socket.WriteAll(std::span(std::to_string(RDKAbad).c_str(), 1));
                break;
            }

            // Key not found
            if (recordIdToOffset.find(idOrRecord) == recordIdToOffset.end()) {
                co_await socket.WriteAll(std::span(std::to_string(RDKAnone).c_str(), 1));
                break;
            }
            auto requestedRecord = readFromWALFileById(idOrRecord);
            co_await socket.WriteAll(std::span(requestedRecord.c_str(), requestedRecord.length()));
            break;
        }

        if (!isUpdate) {
            // Create query
            UUIDv4::UUID uuid = uuidGenerator.getUUID();
            std::string newID = uuid.str();
            std::stringstream walEntry;
            walEntry << "{@" << newID << " " << idOrRecord << "}";
            writeWALToFile(walEntry.str(), newID);
            co_await socket.WriteAll(std::span(newID.c_str(), newID.length()));
        } else {
            // Update query
            writeWALToFile(idOrRecord, idOfRecordToUpdate);
            co_await socket.WriteAll(std::span(idOfRecordToUpdate.c_str(), idOfRecordToUpdate.length()));
        }
    }
}

std::string readFromSSTFileById(const std::string& recordId) {
    return db.get(recordId);
}

std::string readRecordById(const std::string& recordId) {
    std::string walData = readFromWALFileById(recordId);
    
    std::string sstData = readFromSSTFileById(recordId);

    std::string merged = mergeTwoRecords(walData, sstData);

    return merged;
}

// Set up the server and listen for client connections
void startServer() {
    using redka::io::Acceptor;
    using redka::io::Executor;

    struct sockaddr_in serverAddr, clientAddr;
    socklen_t addrLen = sizeof(clientAddr);
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(8080);

    auto acceptor = Acceptor::ListenOn(serverAddr);

    Executor executor(acceptor.get());

    auto acceptTask = [](Executor *executor, Acceptor *acceptor) -> redka::io::CoroResult<void> {
        std::cout << "Server listening on port 8080" << std::endl;
        for (;;) {
            executor->Schedule(handleClient(co_await acceptor->Accept()).fire_and_forgive());
        }
        co_return;
    }(&executor, acceptor.get());

    executor.Schedule(&acceptTask);
    executor.Run();
}

int main() {
    startServer();
    return 0;
}
