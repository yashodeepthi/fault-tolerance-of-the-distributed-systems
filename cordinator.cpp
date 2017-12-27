#include<stdio.h>
#include<stdlib.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<errno.h>
#include<string.h>
#include<unistd.h>
#include<arpa/inet.h>
#include<iostream>
#include<pthread.h>
#include<sstream>
#include<queue>
# include<map>
#include <iomanip>
#include<vector>
#include<string>

using namespace std;
#define SIZE 1000
#define NO_OF_CLIENTS 100
#define NUM_OF_THREADS 20

//global variables
queue<int> thredq;
pthread_t tclients[NUM_OF_THREADS];
pthread_cond_t noful, noemp;
pthread_mutex_t qlock,accountno_lock;
int account_no=0;;

// This converts lowercase string to Uppercase
string toUpperCase(string input){
     string output;
     for(int i=0;i<input.length();i++){
         input[i] = toupper(input[i]);
     }
     return input;
}
//thread for sending broadcast to backend server from front end
void * connectionhandler(void *){
        struct sockaddr_in server_address1, server_address2,server_address3;

        // backend server 1
        server_address1.sin_family = AF_INET;
        server_address1.sin_port = htons(6666);
        server_address1.sin_addr.s_addr =inet_addr("127.0.0.1");
        bzero(&server_address1.sin_zero,8);

        // backend server 2
        server_address2.sin_family = AF_INET;
        server_address2.sin_port = htons(7777);
        server_address2.sin_addr.s_addr =inet_addr("127.0.0.1");
        bzero(&server_address2.sin_zero,8);

        // backend server 3
        server_address3.sin_family = AF_INET;
        server_address3.sin_port = htons(8888);
        server_address3.sin_addr.s_addr =inet_addr("127.0.0.1");
        bzero(&server_address3.sin_zero,8); while(1) {
        int check=0;
        pthread_mutex_lock(&qlock);
        while(thredq.size()==0){
           pthread_cond_wait(&noemp,&qlock);
        }
        int newsock = thredq.front();
        thredq.pop();
        // signalling that request is consumed and queue is not empty
        pthread_cond_signal(&noful);
        pthread_mutex_unlock(&qlock);
        vector<int> sockvector;
        while(1)
        {
        char messagebuff[127];
        memset(messagebuff,'\0',sizeof(messagebuff));
        check++;
        int received = recv(newsock,messagebuff,127,0);
        //cout << "received status: "<< received<<endl;
        if(received<=1){
           break;
        }

        string command="", value1="",value2="",item;
        istringstream iss(messagebuff);
        int i=0;
        while(getline(iss,item,' ')){
                if(i==0){
                  command = item;
                }
                break;
        }

        command = toUpperCase(command);
        cout << "received from client : "<< string(messagebuff)<<endl;
        string q="QUIT";

        if(strcmp(q.c_str(),command.c_str())==0){
                break;
                }

        string servermsg = string(messagebuff);

        string c ="CREATE";
        if(strcmp(c.c_str(),command.c_str())==0){

                pthread_mutex_lock(&accountno_lock);
                account_no++;
                int create_accountno= account_no;
                pthread_mutex_unlock(&accountno_lock);
                servermsg = servermsg +" "+ to_string(create_accountno);

        }


        // connect to 3 backend servers

         int server1_sock = socket(AF_INET, SOCK_STREAM,0);
         int server2_sock = socket(AF_INET, SOCK_STREAM,0);
         int server3_sock = socket(AF_INET, SOCK_STREAM,0);

         if(server1_sock==-1){

                   perror("Error while creating the server 1 socket");
                   exit(-1);
              }
         if(server2_sock==-1){

                   perror("Error while creating the server 2 socket");
                   exit(-1);
              }
         if(server3_sock==-1){

                   perror("Error while creating the server 3 socket");
                   exit(-1);
              }
         // connecting to the servers
         int connect1= connect(server1_sock,(struct sockaddr*)&server_address1,
                                                                sizeof(struct sockaddr_in));
         int connect2= connect(server2_sock,(struct sockaddr*)&server_address2,
                                                                sizeof(struct sockaddr_in));
         int connect3= connect(server3_sock,(struct sockaddr*)&server_address3,
                                                                sizeof(struct sockaddr_in));



         if(connect1!=-1){
                sockvector.push_back(server1_sock);
                int sent1 = send(server1_sock,servermsg.c_str(),127,0);
         }
         if(connect2!=-1){
                sockvector.push_back(server2_sock);
                int sent2 = send(server2_sock,servermsg.c_str(),127,0);
         }
         if(connect3!=-1){
                sockvector.push_back(server3_sock);
                int sent3 = send(server3_sock,servermsg.c_str(),127,0);
         }

         string msg="OK";
         int count=0;
         for(int sock:sockvector){

               //timer implementation
               char messagebuff[127];
               memset(messagebuff,'\0',sizeof(messagebuff));
               struct timeval tv;
               tv.tv_sec = 3;
               tv.tv_usec = 0;
               setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv,sizeof(struct timeval));
               int recv1 = recv(sock,messagebuff,127,0);
               if(recv1<=0){
                        close(sock);
               }


         }
         string cmsg ="commit";
         string done_msg="done";
         int counting_done=0;
         if(sockvector.size()!=0){
              string send_client="";
              for(int sock:sockvector){
                        struct timeval tv;
                        tv.tv_sec = 3;
                        tv.tv_usec = 0;
                        setsockopt (sock, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv,sizeof(timeval));
                        int send1 = send(sock,cmsg.c_str(),127,0);

               }
              char messagebuff[127];
              for(int sock:sockvector){
                        struct timeval tv;
                        tv.tv_sec = 3;
                        tv.tv_usec = 0;
                        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv,sizeof(struct timeval));
                        memset(messagebuff,'\0',sizeof(messagebuff));
                        int recv1 = recv(sock,messagebuff,127,0);

                        if(recv1>0){

                           send_client = string(messagebuff);
                        }

               }

              int send3 = send(newsock,send_client.c_str(),127,0);


              if (send3<= 0)
                {
                        break;
                }

         }



                 for(int sock:sockvector){

                  close(sock);

               }

                sockvector.clear();

        }
        string q1="QUIT";
        int send3 = send(newsock,q1.c_str(),127,0);
        cout << "Done"<< endl;
        close(newsock);
}
}

int main(int argc,char **argv){
       cout << "cordinator server"<< endl;
       int sSocket;
       struct sockaddr_in server_address, client_address;
       unsigned int len;



       sSocket = socket(AF_INET,SOCK_STREAM,0);
       if(sSocket==-1){
             perror("Error: socket cannot be created");
             exit(-1);
        }

        int optval = 1;

        if (setsockopt(sSocket, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int)) < 0){
                         perror("setsockopt(SO_REUSEADDR) failed");
        }


        server_address.sin_family = AF_INET;
        server_address.sin_port = htons(9999);
        server_address.sin_addr.s_addr = INADDR_ANY;
        bzero(&server_address.sin_zero,8);
        len = sizeof(struct sockaddr_in);

        // binding the socket to address and port

        if((bind(sSocket,(struct sockaddr*)&server_address,len))==-1){
                 perror("Error on  binding ");
                 exit(-1);
        }
        if(listen(sSocket,NO_OF_CLIENTS)==-1) {
                 perror("error listening");
                 exit(-1);
        }

         pthread_mutex_init(&qlock,NULL);
         pthread_cond_init(&noful,NULL);
         pthread_cond_init(&noemp,NULL);

        for(int i=0;i<NUM_OF_THREADS;i++){
                 int err =pthread_create(&tclients[i],NULL,connectionhandler,NULL);
        }

        while(1){

           int cSocket = accept(sSocket,(struct sockaddr*)&client_address,&len);
            if( cSocket==-1){

                   perror("error cannot connect to client");
                   exit(-1);
            }

           pthread_mutex_lock(&qlock);
           while(thredq.size()==SIZE){
                //waiting for the signal not full
                pthread_cond_wait(&noful,&qlock);
           }
           thredq.push(cSocket);

           pthread_cond_signal(&noemp);
           pthread_mutex_unlock(&qlock);


        }

//closing server connections
        close(sSocket);
        return 0;
}
