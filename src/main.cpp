#include "net.h"
#include "executor.h"
#include "coro_task.h"

#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>

//#include "uuid_v4.h"

using redka::io::CoroResult;
using redka::io::Acceptor;
using redka::io::Executor;
using redka::io::TcpSocket;

// Hash table: u128 -> std::streampos
std::unordered_map<std::string, std::streampos> recordIdToOffset{};

// Function to write WAL to a log file
void writeWALToFile(const std::string &logEntry, const std::string &filename, std::string const &recordId) {
    std::ofstream logFile;
    logFile.open(filename, std::ios::app);  // Open file in append mode

    if (logFile.is_open()) {
        // Get the current offset and length, update hash table
        std::streampos recordOffset = logFile.tellp();
        recordIdToOffset[recordId] = recordOffset;
        std::cout << recordOffset << std::endl;

        logFile << logEntry << std::endl;
        logFile.close();
    } else {
        std::cerr << "Failed to open WAL file" << std::endl;
    }
}

void readFromWALFileByKey(const std::string &recordKey, const std::string &filename) {
    auto recordPos = recordIdToOffset[recordKey];
    std::ifstream logFile(filename);
    std::string record;

    logFile.seekg(recordPos);
    getline(logFile, record);

    std::cout << "read:" << record << std::endl;
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

        // TODO replace with try_to... with returning false OR change other 'return
        // false' to throw error
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

// Handle the client connection
CoroResult<void> handleClient(TcpSocket socket) {
    std::array<char, 1024> buffer;

    while (true) {
        size_t bytesRead = co_await socket.ReadSome(buffer);
        if (bytesRead <= 0) {
            std::cout << "Client disconnected or error reading" << std::endl;
            break;
        }

        std::string message(buffer.begin(), buffer.begin() + bytesRead);
        std::cout << "Received message: " << message << std::endl;

        // TODO Handle queries (need to implement merge)
        std::string record;
        bool isUpdate = false;
        std::string indexToUpdate;
        if (!parseWriteMessage(message, record, isUpdate, indexToUpdate)) {
            // TODO send to client corresponding error (need to int...)
            std::cerr << "Bad message received, parsing error" << std::endl;

            std::string errorMessage = "Invalid message format";
            co_await socket.WriteAll(std::span(errorMessage.c_str(), errorMessage.length()));
        }

        if (!isUpdate) {
            // TODO Replace with uuid (for easier string representation) - but will
            // need to use another external library
            std::string currentIndex = "b0b-1";

            std::stringstream walEntry;
            walEntry << "{@" << currentIndex << " " << record << "}";
            writeWALToFile(walEntry.str(), "wal.log", currentIndex);
            readFromWALFileByKey(currentIndex, "wal.log");

            co_await socket.WriteAll(std::span(currentIndex.c_str(), currentIndex.length()));
        } else {
            writeWALToFile(record, "wal.log", indexToUpdate);
            readFromWALFileByKey(indexToUpdate, "wal.log");

            co_await socket.WriteAll(std::span(indexToUpdate.c_str(), indexToUpdate.length()));
        }
    }
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


    auto acceptTask = [](Executor* executor, Acceptor* acceptor) -> redka::io::CoroResult<void> {
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
    //UUIDv4::UUIDGenerator<std::mt19937_64> uuidGenerator;
    //UUIDv4::UUID uuid = uuidGenerator.getUUID();
    //std::string uuid_str = uuid.str();
    //std::cout << "Here's a random UUID: " << uuid_str << std::endl;
    startServer();
    return 0;
}
