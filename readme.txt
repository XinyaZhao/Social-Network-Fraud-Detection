1. There are three types of source code: code to profile data; code to ETL data; and code to analyze data.

2. In "Code_to_Analyze” directory:

We use spark for our analysis process. There are four versions of our analysis code, where the fourth version is the final version. You can use frauder_v4.scala to test with the example data we provide.

After you open frauder_v4.scala, please change the input path and output path. This code will generate two output files, with iSet being the suspicious zombie fans, and jSet being customers who purchase these zombie fans.

The result of this example will show a subgraph of this network, where the first 20 followers and first 20 followees are detected as the fraudar network. The other innocent accounts are not included in this subgraph, although they may be followed by the first 20 followers or they have large degrees spreading across this social network.

We provide screenshots when our process is running and successfully completed in the "Code_to_Analyze/screenshot" directory.


3. In "Code_to_Profile" directory:

Due to the convenience for uploading, we did not include all our raw data in this file. Instead, we provide two code files to get the streaming data of two data sources from social network service API and one code file to transform raw data of the third data source obtained from a website(https://snap.stanford.edu/data/egonets-Gplus.html).  For the using of social network APIs, he Auth appID and appSecret may expire at the time you test, so please use a new appID and appSecret to get the data. We also include the data profiling code in this directory

4. In “Code_to_ETL”:

We put all the code used for data cleansing in this directory
