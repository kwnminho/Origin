//
//  Hello World client in C++
//  Connects REQ socket to tcp://localhost:5555
//  Sends "Hello" to server, expects "World" back
//
#include <zmq.hpp>
#include <string>
#include <iostream>
#include <string>

//["MEASURE", "toy", 1416002226, {"toy1": 0.7967693931777731, "toy2": 0.09485023914899271}]
//["REGISTER", "toy", {"toy1": "float", "toy2": "float"}]

using namespace std;

#include <origin-client.h>

int main ()
{
  origin_registration areg = format_registration("cpptoy")
    ("cpptoy1","float")
    ("cpptoy2","float")
    ("cpptoy3","string")
    ("cpptoy4","int");
  string reg=areg.json_string();


  origin_measurement ameas = format_measurement("cpptoy",1416002226)
    ("cpptoy1",3.14)
    ("cpptoy2",123.456)
    ("cpptoy3","foobar")
    ("cpptoy4",8273);
  cout<<ameas.json_string()<<endl;
  string measure = ameas.json_string();
  
    //  Prepare our context and socket
    zmq::context_t context (1);

    zmq::socket_t regsocket (context, ZMQ_REQ);
    std::cout << "Connecting to hello world server…" << std::endl;
    {
      regsocket.connect ("tcp://localhost:5556");
      zmq::message_t request (reg.size());
        memcpy ((void *) request.data (), reg.c_str(), reg.size());
        regsocket.send (request);

        zmq::message_t reply;
        regsocket.recv (&reply);

    }



    {
      zmq::socket_t socket (context, ZMQ_PUSH);
      
      std::cout << "Connecting to hello world server…" << std::endl;
      socket.connect ("tcp://localhost:5557");

      //  Do 10 requests, waiting each time for a response
      std::cout<<"connected"<<std::endl;
      for (int request_nbr = 0; request_nbr != 10; request_nbr++) {
        zmq::message_t request (measure.size());
        memcpy ((void *) request.data (), measure.c_str(), measure.size());
        std::cout << "Sending Hello " << request_nbr << "…" << std::endl;
        socket.send (request);

      //  Get the reply.
      //zmq::message_t reply;
      //      socket.recv (&reply);
      //std::cout << "Received World " << request_nbr << std::endl;
      }
      return 0;
    }
}

