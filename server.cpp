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
#include<fstream>
#include<map>
#include <iomanip>

using namespace std;
#define SIZE 800
#define NO_OF_CLIENTS 80
#define THREAD_NUMS 10

//global variables
int port=0;
queue<int> threadq;
pthread_t tclients[THREAD_NUMS];
pthread_cond_t nfull, noemp;
pthread_mutex_t locks[100000];
pthread_mutex_t qlock,mlock;
map<int, double> accmap;
map<int,int> locmap;
int lock_no=0;
string res ="Response : ";

// This converts lowercase string to Uppercase

string toUpperCase(string s){

     for(int i=0;i<s.length();i++){
         s[i] = toupper(s[i]);
     }
     return s;
}
//thread for handling connections for cordinator server
void * connectionhandler(void *){ while(1) {
        pthread_mutex_lock(&qlock);
        while(threadq.size()==0){
           pthread_cond_wait(&noemp,&qlock);
        }
        int newsock = threadq.front();
        threadq.pop();
        // sending signals queue is not empty
        pthread_cond_signal(&nfull);
        pthread_mutex_unlock(&qlock);

        string clomsg= "close";
        char buff[127];
        memset(buff,'\0',sizeof(buff));


        int recv1 = recv(newsock,buff,127,0);


        if(strcmp(buff,clomsg.c_str())==0){
                break;

        }


        string messages = "OK";
        int sent1 = send(newsock,messages.c_str(),127,0);

        char buffer[127];
        memset(buffer,'\0',sizeof(buffer));
        int recv2 = recv(newsock,buffer,127,0);
        string cmsg ="commit";
        if(strcmp(cmsg.c_str(),buffer)==0){
                cout << "Received : "<< buff<<endl;
                string command, value1,item,value2;
                istringstream iss(buff);
                int i=0;
                while(getline(iss,item,' ')){
                if(i==0){
                  command = item;
                }
                if(i==1){
                  value1 = item;
                }
                if(i==2){
                   value2 = item;
                }

                 i++;
                }

                command = toUpperCase(command);

                string updateC ="UPDATE";
                string CreateC="CREATE";
                string query ="QUERY";
                double amou=0;


                if(strcmp(CreateC.c_str(),command.c_str())==0){

                     amou = atof(value1.c_str());
                     ostringstream oss;
                     oss << setprecision(2)<<showpoint<<fixed<< amou;
                     string temp = oss.str();
                     amou = atof(temp.c_str());
                     if(amou<0){
                                string temp ="Balance cannot be negative";
                                string errormsg = "ERR " + temp ;
                                cout << "Response : " << errormsg << endl;
                                int sendneg = send(newsock,errormsg.c_str(),127,0);
                        }
                     else{
                     pthread_mutex_lock(&mlock);
                     accmap[atoi(value2.c_str())] = amou;
                     lock_no++;
                     locmap[atoi(value2.c_str())] = lock_no;
                     pthread_mutex_unlock(&mlock);
                     string created = "OK " + value2;
                     cout << "Response : " << created <<endl;
                     int sendc = send(newsock,created.c_str(),127,0);
                     }

                }
                else if(strcmp(updateC.c_str(),command.c_str())==0){

                        int accno = atoi(value1.c_str());

                        if(accmap.find( accno ) == accmap.end()){
                                string temp = "Account "+ to_string(accno);
                                string errormsg = "ERR " + temp + " does not exist.";
                                cout <<"Response : " <<errormsg << endl;
                                int sendnot = send(newsock,errormsg.c_str(),127,0);

                        }
                        else
                        {
                        int lockno = locmap[accno];
                        pthread_mutex_lock(&locks[lockno]);
                        amou = atof(value2.c_str());
                        ostringstream oss;
                        oss << setprecision(2)<<fixed<<showpoint<< amou;
                        string temp = oss.str();
                        amou = atof(temp.c_str());
                        if(amou<0){
                                string temp ="Balance cannot be negative";
                                string errormsg = "ERR " + temp ;
                                cout << "Response : " << errormsg << endl;
                                int sendneg = send(newsock,errormsg.c_str(),127,0);
                        }
                        else{
                                accmap[accno] = amou;

                                double bal = accmap[accno];
                                ostringstream oss;
                                oss << setprecision(2)<<fixed<<showpoint<< bal;
                                string temp = oss.str();
                                string updated = "OK " + temp;
                                cout << "Response : " << updated << endl;
                                int sendu = send(newsock,updated.c_str(),127,0);
                           }
                         pthread_mutex_unlock(&locks[lockno]);
                        }

                }
                else if(strcmp(query.c_str(),command.c_str())==0){

                        int accno = atoi(value1.c_str());

                        if(accmap.find( accno ) == accmap.end()){
                                string temp = "Account "+ to_string(accno);
                                string errormsg = "ERR " + temp + " does not exist.";

                                cout << "Response : " << errormsg << endl;
                                int senderr = send(newsock,errormsg.c_str(),127,0);

                        }
                        else{
                                int lockno = locmap[accno];
                                pthread_mutex_lock(&locks[lockno]);
                                double bal = accmap[accno];
                                ostringstream oss2;
                                oss2 << setprecision(2)<<fixed<<showpoint<< bal;
                                string temp = oss2.str();
                                string qmsg = "OK "+ temp;
                                cout << "Response : " << qmsg << endl;
                                int sendq = send(newsock,qmsg.c_str(),127,0);
                                pthread_mutex_unlock(&locks[lockno]);

                        }

                }
                else{
                        string rngCmd = "Please enter the commands CREATE, UPDATE, QUERY, QUIT.";
                        cout << "WRONG COMMAND" << endl;
                        int nwrite = send(newsock,rngCmd.c_str(),127,0);
                }


        }

        cout << "Done"<< endl;
        close(newsock);

        }
}
int main(int argc,char **argv){
       cout << "backend server"<< endl;
       int sSock;
       struct sockaddr_in servaddr, cliaddr;
       unsigned int len;


       port = atoi(argv[1]);
       sSock = socket(AF_INET,SOCK_STREAM,0);
       if(sSock==-1){
             perror("Error server socket cannot be created");
             exit(-1);
        }
        int optvalue = 1;

        if (setsockopt(sSock, SOL_SOCKET, SO_REUSEADDR, &optvalue, sizeof(int)) < 0){
                         perror("setsockopt(SO_REUSEADDR) failed");
        }


        servaddr.sin_family = AF_INET;
        servaddr.sin_port = htons(atoi(argv[1]));
        servaddr.sin_addr.s_addr = INADDR_ANY;
        bzero(&servaddr.sin_zero,8);
        len = sizeof(struct sockaddr_in);

         // binding the socket to address and port

        if((bind(sSock,(struct sockaddr*)&servaddr,len))==-1){
                 perror("Error: cannot bind to the socket address");
                 exit(-1);
        }

        //listening for connections
        if(listen(sSock,NO_OF_CLIENTS)==-1) {
                 perror("error: listening connections error");
                 exit(-1);
        }

         pthread_mutex_init(&qlock,NULL);
         pthread_cond_init(&nfull,NULL);
         pthread_cond_init(&noemp,NULL);

//creating threads

        for(int i=0;i<THREAD_NUMS;i++){
                 int error =pthread_create(&tclients[i],NULL,connectionhandler,NULL);
        }

        while(1){

            int cSock = accept(sSock,(struct sockaddr*)&cliaddr,&len);
            if( cSock==-1){

                   perror("error cannot connect to backend server");
                   exit(-1);
            }

           pthread_mutex_lock(&qlock);
           while(threadq.size()==SIZE){
                //signal not full wait condition
                pthread_cond_wait(&nfull,&qlock);
           }
           threadq.push(cSock);
           pthread_cond_signal(&noemp);
           pthread_mutex_unlock(&qlock);


        }


        close(sSock);
        return 0;
}
