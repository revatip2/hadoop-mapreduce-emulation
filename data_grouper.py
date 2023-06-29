import glob
import heapq
import os
import random
import socket
import sys
import combiner
import time

def grouper(input_file, output_file, buffer_size):
    """
    Sorts the input file into chunks and writes each chunk to a separate file.
    Then, merges the sorted chunks into a single sorted output file.

    :param input_file: the path to the input file to be sorted
    :param output_file: the path to the output file where the sorted data will be written
    :param buffer_size: the number of lines to read from the input file at a time
    """
    chunks = []
    sorted_output = "sorted_output"
    if not os.path.exists(sorted_output):
        os.makedirs(sorted_output)

    with open(input_file, 'r') as file:
        chunk = []
        for line in file:
            word, count = line.strip().split(',')
            chunk.append((word, int(count)))

            if len(chunk) == buffer_size:
                chunk.sort()
                chunk_file = os.path.join(sorted_output, f"chunk{len(chunks)}.txt")
                with open(chunk_file, 'w') as chunk_out:
                    for word, count in chunk:
                        chunk_out.write(f"{word},{count}\n")
                chunks.append(chunk_file)
                chunk = []

        if chunk:
            chunk.sort()
            chunk_file = os.path.join(sorted_output, f"chunk{len(chunks)}.txt")
            with open(chunk_file, 'w') as chunk_out:
                for word, count in chunk:
                    chunk_out.write(f"{word},{count}\n")
            chunks.append(chunk_file)

    with open(output_file, 'w') as sorted_data:
        sorted_chunks = []
        for chunk_file in chunks:
            sorted_chunks.append(open(chunk_file, 'r'))

        for word, count in heapq.merge(*(parse_line(chunk) for chunk in sorted_chunks), key=lambda x: x[0]):
            sorted_data.write(f"{word},{count}\n")

        for chunk in sorted_chunks:
            chunk.close()

def parse_line(file):
    """
    Parses each line of a file and yields a tuple containing the word and its count.

    :param file: the file to be parsed
    """
    for line in file:
        word, count = line.strip().split(',')
        yield word, int(count)


def send_file_to_reducer_server(sorted_file, server_port):
    """
    Sends a sorted file to a reducer server via a TCP socket.

    :param sorted_file: the path to the sorted file to be sent
    :param server_port: the port on which the reducer server is listening
    """
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client.connect(('localhost', server_port))
        client.sendall(sorted_file.encode('utf-8'))
        response = client.recv(1024).decode('utf-8')

        if response == 'OK':
            print(f"[*] Sent {sorted_file} to reducer server on port {server_port}")
        else:
            print(f"[*] Failed to send {sorted_file} to reducer server on port {server_port}")

    except Exception as e:
        print(f"[*] Error connecting to reducer server on port {server_port}: {e}")

    finally:
        client.close()


if __name__ == "__main__":
    shuffled_output_dir = "shuffled_output"
    output_file_name = sys.argv[1]
    num_reducers = int(sys.argv[2])

    buffer_size = 1000
    reducer_files = glob.glob(os.path.join(shuffled_output_dir, "*.out"))

    # Define the ports where the reducer servers are listening
    reducer_ports = [5040 + i for i in range(num_reducers)]

    for index, reducer_file in enumerate(reducer_files):
        output_file = os.path.join("sorted_output", os.path.basename(reducer_file))
        grouper(reducer_file, output_file, buffer_size)
        send_file_to_reducer_server(output_file, reducer_ports[index % len(reducer_ports)])
    time.sleep(10)
    
    reducer_output_dir = 'reducer_output'
    if not os.path.exists(reducer_output_dir):
        os.makedirs(reducer_output_dir)
    combined_output_file = output_file_name
    combiner.map_reduce_combiner(reducer_output_dir, combined_output_file)
