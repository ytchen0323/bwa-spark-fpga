#ifndef MY_SOCKET_H
#define MY_SOCKET_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

int setupSocket( int port )
{
	// socket 
	int err = 0;
	int listenfd = 0;
	socklen_t buf_size = 32*1024*1024;
	socklen_t size = sizeof(buf_size);
	struct sockaddr_in serv_addr; 

	listenfd = socket(AF_INET, SOCK_STREAM, 0);

	err = setsockopt(listenfd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
	err = setsockopt(listenfd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
	err = getsockopt(listenfd, SOL_SOCKET, SO_SNDBUF, &buf_size, &size);
	//printf("socket send buffer size: %d\n", buf_size);

	memset(&serv_addr, '0', sizeof(serv_addr));
	
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	serv_addr.sin_port = htons(port); 

	bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)); 

	listen(listenfd, 24); 

    //printf("socket listening port is ready\n");

    return listenfd;
}

int acceptSocket( int listenfd )
{
    //printf("waiting for host to connect\n");
    int connfd = accept(listenfd, (struct sockaddr*)NULL, NULL); 
    
    if( connfd < 0 )
    {
        //printf("ERROR on accept\n");
        exit(1);
    }
    //printf("host connects, start transfering data.\n");
    return connfd;
}

void recv_large_array( int connfd, char* data, size_t nbyte_exp )
{
    size_t nbyte = 0;
	size_t packet_size = 256*1024;
    unsigned int start;
    for( start = 0; start < nbyte_exp; start += packet_size )
    {
        if( start + packet_size > nbyte_exp ) packet_size = nbyte_exp - start;
        nbyte += recv(connfd, data + start, packet_size, MSG_WAITALL);
    }
    //printf("received data for %d bytes --- \n", (int)nbyte);
}

int recv_param( int connfd )
{
    int temp;
    recv(connfd, &temp, sizeof(int), MSG_WAITALL);
    return temp;
}

void send_large_array( int connfd, char* data, size_t nbyte_exp )
{
    size_t nbyte = 0;
	size_t packet_size = 256*1024;
    unsigned int start;
    for( start = 0; start < nbyte_exp; start += packet_size )
    {
        if( start + packet_size > nbyte_exp ) packet_size = nbyte_exp - start;
        nbyte += send(connfd, data + start, packet_size, 0);
    }
    //printf("sent result for %d bytes --- \n", (int)nbyte);
}

#endif
