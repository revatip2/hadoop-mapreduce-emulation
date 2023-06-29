"""
This script automates the entire MapReduce process by running various subprocesses to execute different tasks.
"""

import ast
import os
import socket
import subprocess
import time
import sys

# get input file, output file, and number of reducers from command-line arguments
input_file = sys.argv[1]
output_file = sys.argv[2]
reducers = sys.argv[3]

#Run the Splitter
print('Splitting the input file..\n')

# spawn a subprocess to run the splitter.py script
server_process = subprocess.Popen(['python3', 'splitter.py', input_file])
server_process.wait()
print('The splits have been stored in the splitter_output directory.')
time.sleep(5)

#Create Mapper Servers
server_processes = []
chunk_dir = "splitter_output"

# spawn a server process for each chunk file in the splitter_output directory
print('Creating mapper servers based on the number of splits generated..\n')
server_process = subprocess.Popen(['python3', 'mapper_server.py'])
server_processes.append(server_process)
time.sleep(1)  # Wait for server to start

#Run the Mappers
# start a mapper process for each chunk file
for i, chunk_filename in enumerate(os.listdir(chunk_dir)):
    port = 8000 + i
    chunk_path = os.path.join(chunk_dir, chunk_filename)
    server_process = subprocess.Popen(['python3', 'mapper.py', chunk_path, str(port)])
    server_process.wait()
print('The mapper outputs have been stored in the mapper_output directory.')
time.sleep(10)


#Run the Partitioner
print('The Partitioner is running..\n')
server_process = subprocess.Popen(['python3', 'partitioner.py', str(reducers)])
server_process.wait()
print('The partitioned outputs are stored in the partitioner_output directory.')
time.sleep(10)

#Run the Shuffler
print('The Shuffler is running..\n')
server_process = subprocess.Popen(['python3', 'data_shuffler.py'])
server_process.wait()
time.sleep(10)

#Set up Reducer Servers
print(f'Setting up {reducers} reducer servers..\n')
server_process = subprocess.Popen(['python3', 'reducer_server.py', str(reducers)])
server_processes.append(server_process)

time.sleep(1)

#Run the Data Grouper
print('The data is sorted and the files are sent to different reducer servers for processing..\n')
server_process = subprocess.Popen(['python3', 'data_grouper.py', output_file, str(reducers)])
server_process.wait()
time.sleep(10)

print(f'The final output file {output_file} is generated successfully.')

# kill any processes that are still listening on ports 5000-5099 and 8000-8099
os.system('lsof -ti :5000-5099 -ti :8000-8099 | xargs kill')




