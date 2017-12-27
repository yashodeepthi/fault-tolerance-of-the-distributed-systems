#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<errno.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<unistd.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<iostream>
#include<fstream>
#include<sstream>
#include<string>

using namespace std;


 int main(int argc , char **argv){
        int cSock;
        struct sockaddr_in server_addr;

        string command;

        //server address

       server_addr.sin_family = AF_INET;
       server_addr.sin_port = htons(atoi(argv[1]));
       server_addr.sin_addr.s_addr =inet_addr(argv[2]);
       bzero(&server_addr.sin_zero,8);


       cSock = socket(AF_INET, SOCK_STREAM,0);

            if(cSock==-1)
              {

                   perror("Error: Cannot create client socket ");
                   exit(-1);
              }
            // connecting to the cordinator server
            if(connect(cSock, (struct sockaddr*)&server_addr,sizeof(struct sockaddr_in))==-1)
              {
                  perror("error cannot connect to the cordinator server");
                  exit(-1);
                }

       // connection established
       cout << "Connection established OK"<<endl;


      string qcond ="QUIT";

      string line;

      while (getline(cin, line)){


            command=line;
            // Write message to the cordinator server
            int nWrite = send(cSock,command.c_str(),127,0);

            char message[200];
            memset(message,'\0',sizeof(message));
            // Read message from cordinator server
            int nRead = recv(cSock,message,sizeof(message),0);
            if(strcmp(message,qcond.c_str())==0){
               break;
            }
            cout << message << endl;
            line="";

       }

//closing client connections
       close(cSock);
       cout << "OK" <<endl;
       cout << "Client Connection closed"<< endl;

}
