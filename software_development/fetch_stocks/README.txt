The task at hand was to fetch the data from aws bucket, filter it for a specific ISIN code, aggregate it on a daily basis, output the data as a csv, including a new column, % change of closing price compared to the previous day.

INSTRUCTIONS (Linux)
1. Download and unpack the zip file in a directory of your choice
2. Navigate to this directory using command line
3. docker build -t my_img .
4. docker run -it my_img bash
5. You are now inside the container. Type the following:
6. python starting.py "DE0005772206" (or any other desired ISIN)
7. Press ​ ctrl+p ​ and ​ ctrl+q ​ after each other to exit the container but keep it running
8. docker cp <container ID here>:/my_project/summary_DE0005772206.csv
/path/on_your/computer/summary.csv
