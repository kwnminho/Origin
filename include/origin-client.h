#ifndef ORIGIN_CLIENT_H
#define ORIGIN_CLIENT_H

#include <string>
#include <vector>
#include <sstream>
#include <stdint.h>

class origin_registration{
  public:
    origin_registration(std::string stream) : stream_(stream){}
    origin_registration() {}

    origin_registration& operator()(std::string name,std::string type){
      names_.push_back(name);
      types_.push_back(type);
      return *this;
    }

  std::string json_string(){
    std::ostringstream ret;
    ret<<"[\""<<stream_<<"\",{";
    for(unsigned i = 0 ; i < names_.size() ; i++){
        ret<<"\""<<names_[i]<<"\":\""<<types_[i]<<"\"";
        if(i!=names_.size()-1)
           ret<<",";
    }
    ret<<"}]";
    return ret.str();
  }

  private:
    std::string stream_;
    std::vector<std::string> names_;
    std::vector<std::string> types_;
};

origin_registration format_registration(std::string stream){
  origin_registration reg(stream);
  return reg;
}

class origin_measurement{
  public:
    origin_measurement(std::string stream,uint64_t t) : stream_(stream),t_(t){}
    origin_measurement() {}

    origin_measurement& operator()(std::string name,double value){
      names_.push_back(name);
      ostringstream valenc;
      valenc<<value;
      values_.push_back(valenc.str());
      return *this;
    }

    origin_measurement& operator()(std::string name,int value){
      names_.push_back(name);
      ostringstream valenc;
      valenc<<value;
      values_.push_back(valenc.str());
      return *this;
    }

    origin_measurement& operator()(std::string name,std::string value){
      names_.push_back(name);
      string valenc = "\"" + value + "\"";
      values_.push_back(valenc);
      return *this;
    }

  std::string json_string(){
    std::ostringstream ret;
    ret<<"[\""<<stream_<<"\","<<t_<<",{";
    for(unsigned i = 0 ; i < names_.size() ; i++){
        ret<<"\""<<names_[i]<<"\":"<<values_[i];
        if(i!=names_.size()-1)
           ret<<",";
    }
    ret<<"}]";
    return ret.str();
  }

  private:
    std::string stream_;
    uint64_t t_;
    std::vector<std::string> names_;
    std::vector<std::string> values_;
};

origin_measurement format_measurement(std::string stream,uint64_t t){
  origin_measurement mes(stream,t);
  return mes;
}

#endif
