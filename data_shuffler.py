import glob
import os


def shuffle_data(partitioner_output_dir):

    # get a list of subdirectories in the partitioner output directory
    reducer_dirs = [f.path for f in os.scandir(partitioner_output_dir) if f.is_dir()]

    # create the shuffled output directory if it does not exist
    shuffled_output_dir = "shuffled_output"
    if not os.path.exists(shuffled_output_dir):
        os.makedirs(shuffled_output_dir)

    num_reducers = len(glob.glob(os.path.join(reducer_dirs[0], "*.out")))

    # iterate over each file and create a shuffled output file for it
    for i in range(num_reducers):
        output_file = os.path.join(shuffled_output_dir, f"r{i}.out")
        with open(output_file, 'w') as outfile:
            for reducer_dir in reducer_dirs:
                input_file = os.path.join(reducer_dir, f"r{i}.out")
                with open(input_file, 'r') as infile:
                    for line in infile:
                        outfile.write(line)

if __name__ == "__main__":
    partitioner_output_dir = "partitioner_output"
    shuffle_data(partitioner_output_dir)
