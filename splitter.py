import os
import sys

def split_file(filename, output_dir, chunk_size=2048):
    """Function to split a file into smaller chunks and save them to a directory.

    Args:
        filename (str): The name of the file to be split.
        output_dir (str): The name of the directory where the chunks will be saved.
        chunk_size (int): The size of each chunk in bytes. Defaults to 2048.

    """
    # Create the output directory if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Open the input file and read its content
    with open(filename, "r") as file:
        file_content = file.read()
        file_size = len(file_content)

        # Calculate the number of chunks required to split the file
        chunks = (file_size // chunk_size) + 1

        # Iterate over each chunk and write it to a separate file
        for i in range(chunks):
            start = i * chunk_size
            end = min((i + 1) * chunk_size, file_size)
            chunk = file_content[start:end]

            # Construct the output file path and write the chunk to it
            output_filename = os.path.join(output_dir, f"chunk_{i}.txt")
            with open(output_filename, "w") as output_file:
                output_file.write(chunk)

if __name__ == "__main__":
    
    # Get the input filename from the command line arguments
    input_filename = sys.argv[1]

     # Set the output directory name
    output_directory = "splitter_output"

    # Call the split_file function with the input file and output directory
    split_file(input_filename, output_directory)
