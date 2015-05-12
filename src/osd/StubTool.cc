#include "StubTool.h"

#include <sstream>
#include <algorithm>

#include <curl/curl.h>
#include <curl/easy.h>
#include <curl/multi.h>

#define dout_subsys ceph_subsys_osd

static size_t send_http_data(char *ptr, size_t size, size_t nmemb, void * _data)
{
  size_t realsize = size * nmemb;
  bufferlist * data = static_cast<bufferlist *>(_data);

  size_t rs = std::min(realsize, (size_t)(data->length()));

  std::ostringstream os;
  data->hexdump(os);

  dout(1) << " [stubtool] sending " << rs
           << " data: " << os.str()
           << dendl;

  data->copy(0, rs, ptr);

  return rs;
}

bool StubTool::in(const hobject_t& soid, const bufferlist& data, std::string * out_url)
{
  bool ret = false;

  std::ostringstream os;
  os << "http://localhost:5000/";
  os << "objects/";
  os << soid;

  std::string url = os.str();

  CURL *curl;
  char error_buf[CURL_ERROR_SIZE];

  curl = curl_easy_init();
  if (curl) {
    dout(1) << "[stubtool] sending object to " << url << dendl;

    curl_easy_setopt(curl, CURLOPT_PUT, 1L);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);

    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    curl_easy_setopt(curl, CURLOPT_READFUNCTION, &send_http_data);
    curl_easy_setopt(curl, CURLOPT_READDATA, (void *) (&data));

    curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, data.length());

    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, (void *)error_buf);

    CURLcode status = curl_easy_perform(curl);

    if (status) {
      dout(1) << "[stubtool] curl_easy_performed returned error: " << error_buf << dendl;
      ret = -EINVAL;
    }
    curl_easy_cleanup(curl);

    ret = true;
    *out_url = url;
  } else {
    ret = false;
  }

  return ret;
}


static size_t retrieve_http_data(char * ptr, size_t size, size_t nmemb, void * _data)
{
  size_t realsize = size * nmemb;
  bufferlist * data = static_cast<bufferlist *>(_data);

  data->append(ptr, realsize);

  std::ostringstream os;
  data->hexdump(os);

  dout(1) << " [stubtool] retrieveing " << realsize << " bytes"
           << " data: " << os.str()
           << dendl;

  return realsize;
}

bool StubTool::out(const std::string& url, bufferlist * out_data)
{
  bool ret = false;

  bufferlist resp_data;

  CURL *curl;
  char error_buf[CURL_ERROR_SIZE];

  curl = curl_easy_init();
  if (curl) {
    dout(1) << "[stubtool] retriving object from " << url << dendl;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &retrieve_http_data);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) &resp_data);

    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, (void *)error_buf);

    CURLcode status = curl_easy_perform(curl);

    if (status) {
      dout(1) << "[stubtool] curl_easy_performed returned error: " << error_buf << dendl;
      ret = -EINVAL;
    }
    curl_easy_cleanup(curl);

    ret = true;
  } else {
    ret = false;
  }

  curl = curl_easy_init();

  if (curl && ret) {
    dout(1) << "[stubtool] deleting object at " << url << dendl;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");

    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, (void *)error_buf);

    CURLcode status = curl_easy_perform(curl);

    if (status) {
      dout(1) << "[stubtool] curl_easy_performed returned error: " << error_buf << dendl;
      ret = -EINVAL;
    }
    curl_easy_cleanup(curl);

    ret = true;
  } else {
    ret = false;
  }

  if (ret) {
    out_data->claim(resp_data);
  }

  return ret;
}
