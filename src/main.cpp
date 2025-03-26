#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>

#include "uuid_v4.h"

// Hash table: u128 -> (std::streampos, u32)
// Why two values? Offset and length -- for faster reading from WAL (and not to
// search for the '\n')
std::unordered_map<std::string, std::tuple<std::streampos, uint32_t>> recordIdToOffset{};

// Function to write WAL to a log file
void writeWALToFile(const std::string &logEntry, const std::string &filename, std::string const &recordId) {
    std::ofstream logFile;
    logFile.open(filename, std::ios::app);  // Open file in append mode

    if (logFile.is_open()) {
        // Get the current offset and length, update hash table
        std::streampos recordOffset = logFile.tellp();
        uint32_t recordLength = logEntry.length();
        recordIdToOffset.insert({recordId, std::make_tuple(recordOffset, recordLength)});
        std::cout << recordOffset << " " << recordLength << std::endl;

        logFile << logEntry << std::endl;
        logFile.close();
    } else {
        std::cerr << "Failed to open WAL file" << std::endl;
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
void handleClient(int clientSocket) {
    char buffer[1024];
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        int bytesRead = read(clientSocket, buffer, sizeof(buffer));
        if (bytesRead <= 0) {
            std::cout << "Client disconnected or error reading" << std::endl;
            break;
        }

        std::string message(buffer);
        std::cout << "Received message: " << message << std::endl;

        // TODO Handle queries (need to implement merge)
        std::string record;
        bool isUpdate = false;
        std::string indexToUpdate;
        if (!parseWriteMessage(message, record, isUpdate, indexToUpdate)) {
            // TODO send to client corresponding error (need to int...)
            std::cerr << "Bad message received, parsing error" << std::endl;

            std::string errorMessage = "Invalid message format";
            send(clientSocket, errorMessage.c_str(), errorMessage.length(), 0);
        }

        if (!isUpdate) {
            // TODO Replace with uuid (for easier string representation) - but will
            // need to use another external library
            std::string currentIndex = "b0b-1";

            std::stringstream walEntry;
            walEntry << "{@" << currentIndex << " " << record << "}";
            writeWALToFile(walEntry.str(), "wal.log", currentIndex);

            send(clientSocket, currentIndex.c_str(), currentIndex.length(), 0);
        } else {
            writeWALToFile(record, "wal.log", indexToUpdate);

            send(clientSocket, indexToUpdate.c_str(), indexToUpdate.length(), 0);
        }
    }
    close(clientSocket);
}

// Set up the server and listen for client connections
void startServer() {
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in serverAddr, clientAddr;
    socklen_t addrLen = sizeof(clientAddr);
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(8080);

    if (bind(serverSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
        perror("Bind failed");
        close(serverSocket);
        exit(EXIT_FAILURE);
    }

    if (listen(serverSocket, 3) < 0) {
        perror("Listen failed");
        close(serverSocket);
        exit(EXIT_FAILURE);
    }

    std::cout << "Server listening on port 8080" << std::endl;

    // Poll-based approach to handle incoming connections
    while (true) {
        int clientSocket = accept(serverSocket, (struct sockaddr *)&clientAddr, &addrLen);
        if (clientSocket < 0) {
            perror("Accept failed");
            continue;
        }
        std::cout << "Client connected" << std::endl;
        handleClient(clientSocket);
    }

    close(serverSocket);
}

int main() {
    UUIDv4::UUIDGenerator<std::mt19937_64> uuidGenerator;
    UUIDv4::UUID uuid = uuidGenerator.getUUID();
    std::string uuid_str = uuid.str();
    std::cout << "Here's a random UUID: " << uuid_str << std::endl;
    startServer();
    return 0;
}