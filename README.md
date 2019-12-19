
# qPyMultithread  


## Table of Contents

- [Installation](#installation)
- [Summary](#summary)
- [Usage](#usage)


## Installation

This script was tested on Python 2.7 No other third party libraries need to be installed.

## Summary 
  This tool is an example of using the practices outlined in the  
 Qualys v2 API User Guide under the Best Practices Section  
 https://www.qualys.com/docs/qualys-api-vmpc-user-guide.pdf  
  
  To improve performance while pulling large sets of data, Multi-threading should be used.  
  Here is an outline of what this script does to while multi-threading to obtain  
  the maximum throughput:  
 1. Make an initial API call to the Host List API endpoint to retrieve all host IDs for the subscription that  
     need to have data retrieved. 

	> Note: Its important to do any filtering on hosts at this point, as filtering during the detection pull  
          can impact performance. Host List API Endpoint:  
              https://<qualysapi url>/api/2.0/fo/asset/host/ 

 2. Break the total Host IDs into batches of a configurable amount (1,000-5,000 is usually good) and send to a Queue.  
 3. Launch X worker threads that will pull the batches from the Queue and launch an API call against:  
  https://qualysapi.url/api/2.0/fo/asset/host/vm/detection/ Using Parameters:  
  
 params = dict( action='list', show_igs=0, show_reopened_info=1, active_kernels_only=1, output_format='XML', status='Active,Re-Opened,New', vm_processed_after=<Date in UTC>, # Formatted as: '2019-04-05T00:00:01Z' truncation_limit = 0, ids=ids )  
  #Considerations  
  
  **Batch size**  
    
   On the backend, the host detection engine will break up the number of hosts to retrieve information on  
   with a maximum size of 10,000. Using a batch size higher than this will not add any benefit to performance. In the same context, there are multiple places that need to pull information so there is an overhead cost regardless of the size being used. For that reason, using a batch size too small can start to hinder performance slightly due to the overhead being used on small requests. Different parameters and the amount of total data on the backend can make requests vary in duration, it is best to experiment with different batch size’s during peak and non-peak hours to determine the optimal size to use.  
  
  **Error Handling**  
    
   Robust error handling and logging is key to any automation and is recommended to implement mechanisms  
   to catch exceptions and retry with exponential back off when errors are encountered. This includes all functions dealing with connection requests, parsing, or writing to disk. Taking care to log as much precise detail as possible so it will be easier to audit later should the need arise.  
  
  **Parsing**  
    
   If an error is encountered, the API will return an error code and a description of the error,  
   which will look like this:  
  
  Simple Return with error:  
  

    <?xml version="1.0" encoding="UTF-8" ?>
    <!DOCTYPE GENERIC_RETURN SYSTEM "https://qualysapi.qualys.com/api/2.0/simple_return.dtd”>
     <SIMPLE_RETURN>
      <RESPONSE>
        <DATETIME>2018-02-14T02:51:36Z</DATETIME>
        <CODE>1234</CODE>
        <TEXT>Description of Error</TEXT>
      </RESPONSE>
    </SIMPLE_RETURN>

  Generic Return with error:

    <?xml version="1.0" encoding="UTF-8" ?>
    <!DOCTYPE GENERIC_RETURN SYSTEM "https://qualysapi.qualys.com/generic_return.dtd">
    <GENERIC_RETURN>
     <API name="index.php" username="username at="2018-02-13T06:09:27Z">
      <RETURN status="FAILED" number="999">Internal error. Please contact customer support.</RETURN>
    </GENERIC_RETURN>
    <!-- Incident signature: 123a12b12c1de4f12345678901a12a12 //-->

  A full list of Error code Responses can be found
  in the API User Guide in Appendix 1
  https://www.qualys.com/docs/qualys-api-vmpc-user-guide.pdf

  Connection Errors
   With retrieving large amounts of data sets and continuously streaming through the API for prolonged periods of time,
   comes the possibility of running into edge cases with regards to connections. Whichever method is used to make the
   outbound connection to the API endpoint, it is recommended to set a timeout to abort/retry a connection if it hasn’t
   been established in a reasonable amount of time. This is to prevent stalling out a thread, resulting in reduced
   performance. Also consider these types of connection errors, amongst others:
   -	Empty Responses
   -	Timeouts
   -	Connection Reset or Internal Error responses. Status codes: 503, 500.
   -	Connection Closed
   These can be caused by either side of the connection, so need to be caught, logged,
   and if they continue then investigated.
   
## Usage

```
Usage: qPyMultiThread.py [options]

Multithreading for Qualys Host Detection v2 API

Options:
  -h, --help  show this help message and exit

  Connection Options:
    -s        Qualys API Server. Defaults to US Platform 1
    -u        Qualys API Username
    -p        Qualys API Password
    -P        Enable/Disable using
    -x        Proxy to use e.g http://localhost:3128

  Configuration Options:
    -a        Number of threads to fetch host assets
    -d        Number of threads to fetch host detections
    -z        Number of threads to fetch Portal HostAssets
    -v        Only pull Data scanned after this date.
    -c        Batch size of Host ID chunks
    -i        Enable pulling data by batches of IPs instead of host ids
    -D        Enable Debug Logging

Examples:
Using a batch size of 500 hosts, with 5 threads in parallel, for all hosts processed since 2019-04-01:

localhost:HostDetection testuser$ python qPyMultiThread.py -u quays_nw93 -c 500 -d 5 -v 2019-04-01
 QG Password:
 2019-04-05 20:16:07-0700 INFO: [qPyMultiThread] Validating Credentials to https://qualysapi.qualys.com/msp/about.php ...
 2019-04-05 20:16:13-0700 INFO: [qPyMultiThread] Got response from Proxy, and msp/about.php endpoint...
 2019-04-05 20:16:13-0700 INFO: [qPyMultiThread] Validation successful, proceeding. verifying limits

```


