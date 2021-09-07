#include "common.h"
#include "BoundedBuffer.h"
#include "Histogram.h"
#include "common.h"
#include "HistogramCollection.h"
#include "TCPreqchannel.h"
#include <thread>
#include <algorithm>
#include <chrono> 
#include <sys/epoll.h>
#include <iostream>
using namespace std;

void patient_thread_function(int n, int pno, BoundedBuffer* request_buffer)
{
    //cerr << "Entering patient thread" << endl;
    datamsg d(pno, 0.0, 1);
    double resp = 0;
    for(int i = 0; i < n; i++){
        /*chan->cwrite(&d, sizeof(datamsg));
        chan->cread(&resp, sizeof(double));
        hc->update(pno, resp);*/
        request_buffer->push((char*)&d, sizeof(datamsg));
        d.seconds += 0.004;
    }
    cerr << "Exiting patient thread" << endl;
}

void file_thread_function(string fname, BoundedBuffer* request_buffer, int mb){
    //cerr << "Entering file_thread_fucntion" << endl;
    //1. create the file
    string recvfname = "recv/" + fname;
    //make it as long as the original length
    char buf [1024];
    filemsg f(0,0);
    memcpy(buf, &f, sizeof(f));
    //cerr << "file_thread_fucntion1" << endl;
    strcpy (buf + sizeof(f), fname.c_str());
    //cerr << "file_thread_fucntion2" << endl;
    //chan->cwrite(buf, sizeof(f) + fname.size() + 1);
    __int64_t filelength;
    //cerr << "file_thread_fucntion3. Filelength is: " << filelength << endl;
    //chan->cread(&filelength, sizeof(filelength));

    //cerr << "Opening up: " << recvfname.c_str() << endl;
    FILE* fp = fopen(recvfname.c_str(), "w");
    //cerr << "file_thread_fucntion5. Filelength is: " << filelength <<  ", fp is: " << fp << endl;
    fseek(fp, filelength, SEEK_SET);
    //cerr << "file_thread_fucntion6" << endl;
    fclose(fp);
    //2. generate all the file messages
    filemsg* fm = (filemsg*) buf;
    //cerr << "file_thread_fucntion7" << endl;
    __int64_t remlen = filelength;

    string filename = "/home/osboxes/Documents/PA5/starter/BIMDC/" + fname;
    FILE *p_file = NULL;
    p_file = fopen(filename.c_str(),"rb");
    fseek(p_file,0,SEEK_END);
    int size = ftell(p_file);
    fclose(p_file);
    //cerr << size << endl;

    remlen = size;
    cerr << "remlen: " << remlen << endl;
    while(remlen > 0){
        fm->length = min (remlen, (__int64_t) mb);
        request_buffer->push(buf, sizeof(filemsg) + fname.size() + 1);
        fm->offset += fm->length;
        remlen -= fm->length;
    }
    cerr << "Exiting file_thread_fucntion" << endl;
}

//n, p, w, m, wchans, &request_buffer, &hc
void event_polling_function(int n, int p, int w, int mb, TCPRequestChannel** wchans, BoundedBuffer* request_buffer, HistogramCollection* hc)
{
    char buf [1024];
    double resp = 0;

    char recvbuf [mb];

    struct epoll_event ev;
    struct epoll_event events[w];

    //create an empty epoll list
    int epollfd = epoll_create1(0);
    if(epollfd == -1){
        EXITONERROR("epoll_create1");
    }

    unordered_map<int, int> fd_to_index;
    vector<vector<char>> state (w);

    int nsent = 0, nrecv = 0;
    bool quit_recv = false;
    //priming + adding each ffd to the list 
    cerr << "Entering Priming" << endl;
    for(int i = 0; i < w; i++){
        //err << "Priming loop is on i: " << i << endl;
        int sz = request_buffer->pop(buf, 1024);
        //cerr << "Prime1" << endl;
        if(*(MESSAGE_TYPE*) buf == QUIT_MSG){
            quit_recv = true;
            break; 
        }
        //cerr << "Prime2" << endl;
        wchans[i]->cwrite(buf, sz);
        //cerr << "Prime3" << endl;
        state[i] = vector<char>(buf, buf+sz); //record the state [i]
        //cerr << "Prime4" << endl;
        nsent++;
        //cerr << "Prime5" << endl;
        int rfd = wchans[i]->getfd();
        fcntl(rfd, F_SETFL, O_NONBLOCK);

        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = rfd;
        fd_to_index [rfd] = i;
        //cerr << "Prime6" << endl;
        if(epoll_ctl (epollfd, EPOLL_CTL_ADD, rfd, &ev) == -1){
            EXITONERROR("epoll_ctl: listen_sock");
        }
    }
    cerr << "Exited Priming" << endl;
    //nsent = w, nrecvd = 0;
    //bool quit_recv = false;

    while(true)
    {
        //cerr << "quit_recv: " << quit_recv << ", nrecv: " << nrecv << ", nsent: " << nsent << endl;
        if(quit_recv && nrecv == nsent){
            cerr << "Break called" << endl;
            break;
        }
        //cerr << "On top of epoll_wait" << endl;
        int nfds = epoll_wait(epollfd, events, w, -1);
        //cerr << "Below epoll_wait" << endl;
        if(nfds == -1){
            EXITONERROR ("epoll_wait");
        }
        //cerr << "Trace1" << endl;
        for(int i = 0; i < nfds; i++)
        {
            int rfd = events[i].data.fd;
            int index = fd_to_index[rfd];

            //cerr << "Trace2"<< endl;
            int resp_sz = wchans [index]->cread (recvbuf, mb);
            nrecv ++;

            //cerr << "Trace2.1"<< endl;
            //process (recvbuf)
            vector<char> req = state [index];
            char* request = req.data();
            //processing the response
            MESSAGE_TYPE* m = (MESSAGE_TYPE *) request;
            if(*m == DATA_MSG){
                hc->update(((datamsg *)request)-> person, *(double*)recvbuf);
            }else if(*m == FILE_MSG)
            {
                //cerr << "Entered if FILE_MSG" << endl;
                filemsg* fm = (filemsg*) m;
                string fname = (char*)(fm + 1);

                string recvfname = "recv/" + fname;
                //cerr << "recvfname: " << recvfname << endl;
                FILE* fp = fopen(recvfname.c_str(), "r+");
                fseek(fp, fm->offset, SEEK_SET);
                fwrite(recvbuf, 1, fm->length, fp);
                //cerr << "fm->offset: " << fm->offset << ", fm->lenth: " << fm->length << endl;
                fclose(fp);
            }
            //cerr << "Trace3" << endl;
            // reuse 
            if(!quit_recv)
            {
                int req_sz = request_buffer->pop(buf, sizeof(buf));
                if(*(MESSAGE_TYPE*) buf == QUIT_MSG){
                    cerr << "Quit Received" << endl;
                    quit_recv = true;
                }
                else
                {
                    //cerr << "Trace5" << endl;
                    wchans [index]->cwrite(buf, req_sz);
                    state[index] = vector<char> (buf, buf+req_sz);
                    nsent++;
                }
            }
        }
    }
    //cerr << "Broke out of infinite loop" << endl;
}


int main(int argc, char *argv[])
{
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    int n = 15000;    //default number of requests per "patient"
    int p = 1;     // number of patients [1,15]
    int w = 200;    //default number of worker threads
    int b = 5000; 	// default capacity of the request buffer, you should change this default
	int m = MAX_MESSAGE; 	// default capacity of the message buffer
    string fname;           //default filename is nothing
    bool requestFile = false;
    bool serverBufferCapacityChanged = false;
    srand(time_t(NULL));
    int c = -1;
    string host, port;
    while ((c = getopt (argc, argv, "m:n:b:w:p:h:r:f:")) != -1){
        switch (c){
            case 'm':
                m = atoi (optarg);
                cerr << "m is: " << m << endl;
                break;
            case 'n':
                n = atoi(optarg);
                cerr << "n is: " << n << endl;
                break;
            case 'p':
                p = atoi (optarg);
                cerr << "p is: " << p << endl;
                break;
            case 'b':
                b = atoi (optarg);
                cerr << "b is: " << b << endl;
                break;
            case 'w':
                w = atoi(optarg);
                cerr << "w is: " << w << endl;
                break;
            case 'h':
                host = optarg;
                //cerr << "w is: " << w << endl;
                break;
            case 'r':
                port = optarg;
                //cerr << "w is: " << w << endl;
                break;
            case 'f':
                requestFile = true;
                fname = optarg;
                cerr << "fname: " << optarg << endl;
                break;
        }
    }
    //TCPRequestChannel* chan = new TCPRequestChannel("control", port);
	BoundedBuffer request_buffer (b);
	HistogramCollection hc;
	
    //making histograms and adding to the histogram collection hc
    for(int i = 0; i < p; i++){
        Histogram* h = new Histogram(10, -2.0, 2.0);
        hc.add(h);
    }
	
    //make w worker channels
    cerr << "Making worker channels" << endl;
    TCPRequestChannel** wchans = new TCPRequestChannel* [w];
    for(int i = 0; i < w; i++){
        wchans[i] = new TCPRequestChannel (host, port);
    }
	
    //std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    /*Start all threads here*/
    //cerr << "Entering Threading section" << endl;
    if(!requestFile){
        thread patient [p];
        for(int i = 0; i<p; i++){
            patient [i] = thread (patient_thread_function, n, i+1, &request_buffer);
        }

        thread evp (event_polling_function, n, p, w, m, wchans, &request_buffer, &hc);   
    
        cerr << "Entering patient join" << endl;
        for(int i = 0; i < p; i++){
            cerr << "Waiting for patient join of: " << i << endl;
            patient[i].join();
        }
        MESSAGE_TYPE q = QUIT_MSG;
        request_buffer.push((char*) &q, sizeof(q));
        evp.join();
        cerr << "Worker threads finished" << endl;
        // print the results
        hc.print ();
    }
    else
    {
         cerr << "Entering else (!requestFile)" << endl;
         thread filethread (file_thread_function, fname, &request_buffer, m);
         //cerr << "Below filethread() from main" << endl;       
        thread evp (event_polling_function, n, p, w, m, wchans, &request_buffer, &hc);  

        /* Join all threads here */
        //cerr << "Above filethread Join" << endl;
        filethread.join();
        //cerr << "Below filethread.join() from main" << endl;
        //cerr << "Sending Quit message for file transfer" << endl;
        MESSAGE_TYPE q = QUIT_MSG;
        request_buffer.push((char*) &q, sizeof(q));
        evp.join();
        //cerr << "Worker threads finished" << endl;
    }





    
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cerr << "Time difference = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "[milliseconds] for worker threads: " << w << std::endl;
     
    for(int i = 0; i < w; i++){
        //cerr << "loop: " << i << endl;
        MESSAGE_TYPE q = QUIT_MSG;
        wchans[i]->cwrite((char *) &q, sizeof (MESSAGE_TYPE));
    }
}
