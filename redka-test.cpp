#include <iostream>
#include <string>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

using namespace std;


int main() {
  // Server details
  const char* serverIP = "127.0.0.1"; // Localhost IP address
  int serverPort = 8080;

  // Create socket
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    perror("Socket creation failed");
    return -1;
  }

  // Server address setup
  struct sockaddr_in serverAddr;
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(serverPort);
  if (inet_pton(AF_INET, serverIP, &serverAddr.sin_addr) <= 0) {
    perror("Invalid address");
    close(sock);
    return -1;
  }

  // Connect to the server
  if (connect(sock, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
    perror("Connection failed");
    close(sock);
    return -1;
  }
  cout << "Connected to the server" << endl;

  // Prepare the message
  string message = R"({name:"Alice"})";
  // string message = R"({@6e88d1ce-ddd4-4a97-8e96-29a00adfc8a1 surname:"Liddell" address:"Wonderland"})";
  // string message = R"({@6e88d1ce-ddd4-4a97-8e96-29a00adfc8a1 address@2:"Home"})";
  // string message = R"(6e88d1ce-ddd4-4a97-8e96-29a00adfc8a1)";

  // Send the message to the server
  send(sock, message.c_str(), message.length(), 0);
  cout << "Message sent: " << message << endl;

  // Receive the server's response
  char buffer[1024];
  memset(buffer, 0, sizeof(buffer));
  int bytesRead = read(sock, buffer, sizeof(buffer));
  if (bytesRead > 0) {
    cout << "Server response: " << string(buffer, bytesRead) << endl;
  } else {
    cout << "Error receiving response" << endl;
  }

  // Close the connection
  close(sock);
  return 0;
}